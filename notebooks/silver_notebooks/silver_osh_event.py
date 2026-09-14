# Databricks notebook source
# ============================================================================
#    ___  ____  _   _
#   / _ \/ ___|| | | |      OAK STREET HEALTH
#  | | | \___ \| |_| |      Appointment Reminder Pipeline
#  | |_| |___) |  _  |      Databricks Asset Bundle / Medallion
#   \___/|____/|_| |_|
# ============================================================================
#  Notebook : silver_osh_event
#  Author   : eddie turner
#  Date     : 2026-09-13
#  Details  : Casts the bronze VARIANT payload into typed, normalised columns.
#             Merges on Id plus bronze load time; keeps sms_consent = true only.
# ============================================================================
from __future__ import annotations

import re
from pyspark.sql import SparkSession


def get_widget(name: str, default: str = "") -> str:
    dbutils.widgets.text(name, default)
    value = dbutils.widgets.get(name)
    return value.strip() if value else default


catalog_env = get_widget("catalog_env", "dev")
source_table = get_widget("source_table")
target_table = get_widget("target_table")

for name, value in (
    ("catalog_env", catalog_env),
    ("source_table", source_table),
    ("target_table", target_table),
):
    if not value:
        raise ValueError(f"{name} must be supplied by silver_ingestion.")

table_pattern = r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){2}"
for name, value in (("source_table", source_table), ("target_table", target_table)):
    if not re.fullmatch(table_pattern, value):
        raise ValueError(f"{name} must be a plain catalog.schema.table name, got {value!r}")

spark = SparkSession.builder.getOrCreate()
table_name = target_table.split(".")[-1]

# COMMAND ----------
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
      Id BIGINT NOT NULL,
      patient_signature STRING,
      risk_level STRING,
      risk_category STRING,
      sms_consent BOOLEAN,
      appointment_status STRING,
      cohort STRING,
      last_pcp_visit_date DATE,
      web_logic_time TIMESTAMP,
      web_submission_time TIMESTAMP,
      approval_time TIMESTAMP,
      bronze_layer_timestamp TIMESTAMP,
      silver_layer_timestamp TIMESTAMP,
      CONSTRAINT {table_name}_pk PRIMARY KEY (Id)
    )
    USING DELTA
    """
)

# COMMAND ----------
spark.sql(
    f"""
    MERGE INTO {target_table} t
    USING (
      SELECT
        Id,
        payload:patient_signature::string                      AS patient_signature,
        payload:risk_level::string                             AS risk_level,
        payload:risk_category::string                          AS risk_category,
        payload:sms_consent::boolean                           AS sms_consent,
        payload:appointment_status::string                     AS appointment_status,
        payload:cohort::string                                 AS cohort,
        CAST(payload:last_pcp_visit_date::string AS DATE)      AS last_pcp_visit_date,
        CAST(payload:web_logic_time::string AS TIMESTAMP)      AS web_logic_time,
        CAST(payload:web_submission_time::string AS TIMESTAMP) AS web_submission_time,
        CAST(payload:approval_time::string AS TIMESTAMP)       AS approval_time,
        bronze_layer_timestamp,
        current_timestamp()                                    AS silver_layer_timestamp
      FROM {source_table}
      WHERE payload:sms_consent::boolean = true
    ) s
      ON t.Id = s.Id
     AND t.bronze_layer_timestamp = s.bronze_layer_timestamp
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

# COMMAND ----------
row_count = spark.sql(f"SELECT count(*) AS row_count FROM {target_table}").collect()[0]["row_count"]
print(f"{target_table} now holds {row_count} row(s)")
dbutils.notebook.exit(str(row_count))
