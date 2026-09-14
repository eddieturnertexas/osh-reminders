# Databricks notebook source
# ============================================================================
#    ___  ____  _   _
#   / _ \/ ___|| | | |      OAK STREET HEALTH
#  | | | \___ \| |_| |      Appointment Reminder Pipeline
#  | |_| |___) |  _  |      Databricks Asset Bundle / Medallion
#   \___/|____/|_| |_|
# ============================================================================
#  Notebook : silver_ingestion
#  Author   : eddie turner
#  Date     : 2026-09-13
#  Details  : Runs each enabled silver stream in RunOrder from its config row.
#             Dispatches to the notebook the config names, then logs the result.
# ============================================================================

# COMMAND ----------
from __future__ import annotations

import os
import re
import traceback
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType


def get_widget(name: str, default: str) -> str:
    dbutils.widgets.text(name, default)
    value = dbutils.widgets.get(name)
    return value.strip() if value else default


def sql_literal(value: str) -> str:
    return value.replace("'", "''")


def sql_quote(value: str) -> str:
    return "'" + sql_literal(value) + "'"


def resolve_catalog(prefix_value: str, configured_catalog: str) -> str:
    catalog = str(configured_catalog).strip()
    return catalog if catalog.startswith(f"{prefix_value}_") else f"{prefix_value}{catalog}"


catalog_env = get_widget("catalog_env", "dev")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog_env):
    raise ValueError(f"Invalid catalog_env: {catalog_env!r}")

pipeline_name = get_widget("job_name", "Test")
run_id = get_widget("run_id", "Test")
job_id = get_widget("job_id", "Test")
task_name = get_widget("task_name", "Test")
task_run_id = get_widget("task_run_id", "Test")

spark = SparkSession.builder.getOrCreate()
prefix = catalog_env

JOB_ID = os.environ.get("DATABRICKS_JOB_ID", job_id)
RUN_ID = os.environ.get("DATABRICKS_RUN_ID", run_id)
SILVER_STATUS_TABLE = f"{prefix}_osh_bronze_db.osh.osh_config_silver_status"
SILVER_STREAM_TABLE = f"{prefix}_osh_bronze_db.osh.osh_config_silver_stream"
HEADER_LOG_TABLE = f"{prefix}_osh_bronze_db.osh.osh_config_log_header"
INGESTION_LOG_TABLE = f"{prefix}_osh_bronze_db.osh.osh_config_log_header_detail"

CHILD_TIMEOUT_SECONDS = 3600

log_schema = StructType(
    [
        StructField("StreamName", StringType(), False),
        StructField("PipelineName", StringType(), False),
        StructField("TaskName", StringType(), False),
        StructField("TaskRunId", StringType(), False),
        StructField("EventType", StringType(), False),
        StructField("Status", StringType(), False),
        StructField("BatchId", LongType(), True),
        StructField("RowCount", LongType(), True),
        StructField("FileCount", LongType(), True),
        StructField("JobId", StringType(), True),
        StructField("RunId", StringType(), True),
        StructField("Message", StringType(), True),
        StructField("EventTimestamp", StringType(), True),
    ]
)


def log_event(stream, et, st, batch_id=None, rc=None, fc=None, msg=None):
    (
        spark.createDataFrame(
            [(stream, pipeline_name, task_name, task_run_id, et, st, batch_id, rc, fc, JOB_ID, RUN_ID, msg, None)],
            log_schema,
        )
        .withColumn("EventTimestamp", F.current_timestamp())
        .write.mode("append")
        .format("delta")
        .saveAsTable(INGESTION_LOG_TABLE)
    )


def write_header(status: str) -> None:
    user_name = spark.sql("SELECT current_user() AS user_name").collect()[0]["user_name"]
    spark.sql(
        f"""
        INSERT INTO {HEADER_LOG_TABLE} (
          DbxJobId,
          DbxRunId,
          JobName,
          PipelineName,
          TaskName,
          TaskRunId,
          Status,
          UserName,
          EventTimestamp
        )
        VALUES (
          '{sql_literal(JOB_ID)}',
          '{sql_literal(RUN_ID)}',
          '{sql_literal(pipeline_name)}',
          '{sql_literal(pipeline_name)}',
          '{sql_literal(task_name)}',
          '{sql_literal(task_run_id)}',
          '{sql_literal(status)}',
          '{sql_literal(user_name)}',
          current_timestamp()
        )
        """
    )


# COMMAND ----------
write_header("Silver Started")

streams = spark.sql(
    f"""
    SELECT StreamName, IsSilverEnabled
    FROM {SILVER_STATUS_TABLE}
    ORDER BY RunOrder
    """
).collect()

print(f"{len(streams)} stream(s) registered for silver")

for stream_row in streams:
    current_stream = stream_row.StreamName

    if stream_row.IsSilverEnabled != 1:
        log_event(current_stream, "SKIPPED", "DISABLED", msg="Stream is disabled; nothing loaded")
        print(f"{current_stream}: disabled, skipped")
        continue

    phase = "config_lookup"
    try:
        cfg = (
            spark.table(SILVER_STREAM_TABLE)
            .filter(f"StreamName = '{current_stream}' AND IsActive = 1")
            .collect()[0]
        )

        source_catalog = resolve_catalog(prefix, cfg.SourceCatalog)
        target_catalog = resolve_catalog(prefix, cfg.TargetCatalog)
        source_table = f"{source_catalog}.{cfg.SourceSchema}.{cfg.SourceTableName}"
        target_table = f"{target_catalog}.{cfg.TargetSchema}.{cfg.TargetTableName}"
        notebook_path = f"{cfg.SourceNotebookPath.rstrip('/')}/{cfg.SourceNotebookName}"

        phase = "set_running"
        spark.sql(
            f"""
            UPDATE {SILVER_STATUS_TABLE}
            SET IsSilverRunning = 1,
                IsSilverCompleted = 0,
                LastModified = current_timestamp()
            WHERE StreamName = {sql_quote(current_stream)}
              AND IsSilverEnabled = 1
            """
        )

        phase = "detail_start"
        log_event(current_stream, "START", "STARTED", msg=f"Running {notebook_path}")

        phase = "notebook_run"
        result = dbutils.notebook.run(
            notebook_path,
            CHILD_TIMEOUT_SECONDS,
            {
                "catalog_env": catalog_env,
                "source_table": source_table,
                "target_table": target_table,
            },
        )

        phase = "detail_progress"
        row_count = int(result) if str(result).strip().lstrip("-").isdigit() else None
        log_event(
            current_stream,
            "PROGRESS",
            "RUNNING",
            rc=row_count,
            msg=f"{notebook_path} loaded {target_table} from {source_table}; result={result}",
        )

        phase = "set_completed"
        spark.sql(
            f"""
            UPDATE {SILVER_STATUS_TABLE}
            SET IsSilverRunning = 0,
                IsSilverCompleted = 1,
                LastModified = current_timestamp()
            WHERE StreamName = {sql_quote(current_stream)}
            """
        )

        phase = "detail_complete"
        log_event(current_stream, "STOP", "COMPLETED", msg="Silver load completed")

    except Exception as e:
        spark.sql(
            f"""
            UPDATE {SILVER_STATUS_TABLE}
            SET IsSilverRunning = 0,
                IsSilverCompleted = 0,
                LastModified = current_timestamp()
            WHERE StreamName = {sql_quote(current_stream)}
            """
        )
        log_event(
            current_stream,
            "ERROR",
            "FAILED",
            msg=(
                f"phase={phase}; error={str(e)}; "
                f"trace={traceback.format_exc(limit=20)} | continuing with next stream"
            ),
        )
        continue

write_header("Silver Ended")
