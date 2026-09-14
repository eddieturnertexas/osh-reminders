# Databricks notebook source
# ============================================================================
#    ___  ____  _   _
#   / _ \/ ___|| | | |      OAK STREET HEALTH
#  | | | \___ \| |_| |      Appointment Reminder Pipeline
#  | |_| |___) |  _  |      Databricks Asset Bundle / Medallion
#   \___/|____/|_| |_|
# ============================================================================
#  Notebook : bronze_ingestion
#  Author   : eddie turner
#  Date     : 2026-09-13
#  Details  : Pages the OSH API in 200-row micro-batches up to 2000 per run.
#             Each batch commits on its own; records land in one VARIANT column.
# ============================================================================

# COMMAND ----------
from __future__ import annotations

import json
import os
import re
import traceback
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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


def resolve_target_catalog(prefix_value: str, configured_catalog: str) -> str:
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

api_endpoint = get_widget("api_endpoint", "payload").strip("/")

spark = SparkSession.builder.getOrCreate()
prefix = catalog_env

JOB_ID = os.environ.get("DATABRICKS_JOB_ID", job_id)
RUN_ID = os.environ.get("DATABRICKS_RUN_ID", run_id)
BRONZE_STATUS_TABLE = f"{prefix}_osh_bronze_db.osh.osh_config_bronze_status"
BRONZE_STREAM_TABLE = f"{prefix}_osh_bronze_db.osh.osh_config_bronze_stream"
HEADER_LOG_TABLE = f"{prefix}_osh_bronze_db.osh.osh_config_log_header"
INGESTION_LOG_TABLE = f"{prefix}_osh_bronze_db.osh.osh_config_log_header_detail"

PAYLOAD_RECORDS_KEY = "patients"
API_TIMEOUT_SECONDS = 60

PAYLOAD_COLUMN = "payload"

API_BATCH_SIZE = int(get_widget("batch_size", "200"))
API_MAX_ROWS = int(get_widget("max_rows", "2000"))
API_MAX_RETRIES = 4
API_BACKOFF_SECONDS = 2.0

if API_BATCH_SIZE < 1:
    raise ValueError(f"batch_size must be at least 1, got {API_BATCH_SIZE}")
if API_MAX_ROWS < 1:
    raise ValueError(f"max_rows must be at least 1, got {API_MAX_ROWS}")

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


def build_bronze_frame(records: list):
    rows = [(json.dumps(record, sort_keys=True),) for record in records]
    return (
        spark.createDataFrame(rows, "payload_json STRING")
        .withColumn("Id", F.expr("xxhash64(payload_json)"))
        .withColumn(PAYLOAD_COLUMN, F.expr("parse_json(payload_json)"))
        .withColumn("bronze_layer_timestamp", F.current_timestamp())
        .select("Id", PAYLOAD_COLUMN, "bronze_layer_timestamp")
    )


def write_bronze_incremental(frame, target_table: str) -> None:
    if not spark.catalog.tableExists(target_table):
        frame.write.format("delta").saveAsTable(target_table)
        return

    batch_view = "bronze_incoming_batch"
    frame.createOrReplaceTempView(batch_view)
    try:
        spark.sql(
            f"""
            MERGE INTO {target_table} t
            USING {batch_view} s
              ON t.Id = s.Id
            WHEN NOT MATCHED THEN
              INSERT *
            """
        )
    finally:
        spark.catalog.dropTempView(batch_view)


def resolve_api_credentials(config_row):
    address_scope, address_key = config_row.ApiAdress.split("/", 1)
    token_scope, token_key = config_row.ApiToken.split("/", 1)
    base_url = dbutils.secrets.get(scope=address_scope, key=address_key).strip().rstrip("/")
    token = dbutils.secrets.get(scope=token_scope, key=token_key)
    return base_url, token


def build_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=API_MAX_RETRIES,
        backoff_factor=API_BACKOFF_SECONDS,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=2, pool_maxsize=4))
    return session


def fetch_payload_page(session, base_url: str, token: str, limit: int, offset: int) -> list:
    response = session.post(
        f"{base_url}/{api_endpoint}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
        },
        params={"limit": limit, "offset": offset},
        timeout=API_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json().get(PAYLOAD_RECORDS_KEY) or []


# COMMAND ----------
http_session = build_http_session()

write_header("Bronze Started")

streams = spark.sql(
    f"""
    SELECT StreamName, IsBronzeEnabled
    FROM {BRONZE_STATUS_TABLE}
    ORDER BY RunOrder
    """
).collect()

print(f"{len(streams)} stream(s) registered for bronze")

for stream_row in streams:
    current_stream = stream_row.StreamName

    if stream_row.IsBronzeEnabled != 1:
        log_event(current_stream, "SKIPPED", "DISABLED", msg="Stream is disabled; nothing ingested")
        print(f"{current_stream}: disabled, skipped")
        continue

    phase = "config_lookup"
    try:
        cfg = (
            spark.table(BRONZE_STREAM_TABLE)
            .filter(f"StreamName = '{current_stream}' AND IsActive = 1")
            .collect()[0]
        )

        target_catalog = resolve_target_catalog(prefix, cfg.TargetCatalog)
        target_table = f"{target_catalog}.{cfg.TargetSchema}.{cfg.TargetTableName}"

        phase = "set_running"
        spark.sql(
            f"""
            UPDATE {BRONZE_STATUS_TABLE}
            SET IsBronzeRunning = 1,
                IsBronzeCompleted = 0,
                LastModified = current_timestamp()
            WHERE StreamName = {sql_quote(current_stream)}
              AND IsBronzeEnabled = 1
            """
        )

        phase = "detail_start"
        log_event(current_stream, "START", "STARTED", msg="Ingestion started")

        phase = "api_credentials"
        base_url, token = resolve_api_credentials(cfg)

        offset = 0
        batch_number = 0
        row_count = 0
        stop_reason = "max_rows reached"

        while row_count < API_MAX_ROWS:
            requested = min(API_BATCH_SIZE, API_MAX_ROWS - row_count)

            phase = f"api_fetch_batch_{batch_number + 1}"
            records = fetch_payload_page(http_session, base_url, token, requested, offset)
            if not records:
                stop_reason = "API returned an empty page"
                break

            batch_number += 1
            phase = f"bronze_write_batch_{batch_number}"
            write_bronze_incremental(build_bronze_frame(records), target_table)

            returned = len(records)
            row_count += returned
            offset += returned

            log_event(
                current_stream,
                "PROGRESS",
                "RUNNING",
                batch_id=batch_number,
                rc=returned,
                msg=f"Batch {batch_number}: {returned} records into {target_table}, running total {row_count}",
            )
            print(f"{current_stream} batch {batch_number}: {returned} records, running total {row_count}")

            if returned < requested:
                stop_reason = "API returned a short page; source exhausted"
                break

        phase = "detail_summary"
        log_event(
            current_stream,
            "SUMMARY",
            "COMPLETED",
            fc=batch_number,
            rc=row_count,
            msg=(
                f"{batch_number} micro-batch(es), {row_count} records into {target_table}; {stop_reason}"
                if row_count
                else f"No data in payload; {stop_reason}"
            ),
        )

        phase = "set_completed"
        spark.sql(
            f"""
            UPDATE {BRONZE_STATUS_TABLE}
            SET IsBronzeRunning = 0,
                IsBronzeCompleted = 1,
                LastModified = current_timestamp()
            WHERE StreamName = {sql_quote(current_stream)}
            """
        )

        phase = "detail_complete"
        log_event(current_stream, "STOP", "COMPLETED", msg="Ingestion completed")

    except Exception as e:
        spark.sql(
            f"""
            UPDATE {BRONZE_STATUS_TABLE}
            SET IsBronzeRunning = 0,
                IsBronzeCompleted = 0,
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

write_header("Bronze Ended")
