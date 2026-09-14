# Databricks notebook source
# ============================================================================
#    ___  ____  _   _
#   / _ \/ ___|| | | |      OAK STREET HEALTH
#  | | | \___ \| |_| |      Appointment Reminder Pipeline
#  | |_| |___) |  _  |      Databricks Asset Bundle / Medallion
#   \___/|____/|_| |_|
# ============================================================================
#  Notebook : gold_osh_reminder
#  Author   : eddie turner
#  Date     : 2026-09-13
#  Details  : Builds the reminder fact from silver joined to the three dimensions.
#             Adds patient, location and calendar attributes plus campaign measures.
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
dim_schema = get_widget("dim_schema", "osh_reminder")

for name, value in (
    ("catalog_env", catalog_env),
    ("source_table", source_table),
    ("target_table", target_table),
    ("dim_schema", dim_schema),
):
    if not value:
        raise ValueError(f"{name} must be supplied by gold_ingestion.")

table_pattern = r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){2}"
for name, value in (("source_table", source_table), ("target_table", target_table)):
    if not re.fullmatch(table_pattern, value):
        raise ValueError(f"{name} must be a plain catalog.schema.table name, got {value!r}")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", dim_schema):
    raise ValueError(f"dim_schema must be a plain schema name, got {dim_schema!r}")

spark = SparkSession.builder.getOrCreate()
table_name = target_table.split(".")[-1]

dim_namespace = f"{source_table.split('.')[0]}.{dim_schema}"
dim_patients = f"{dim_namespace}.dim_patients"
dim_location = f"{dim_namespace}.dim_location"
dim_date = f"{dim_namespace}.dim_date"

# COMMAND ----------
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
      Id BIGINT NOT NULL,
      patient_key BIGINT,
      location_key INT,
      submission_date_key INT,
      last_pcp_visit_date_key INT,
      patient_signature STRING,
      patient_id STRING,
      full_name STRING,
      gender STRING,
      phone_number STRING,
      patient_city STRING,
      patient_state STRING,
      patient_zip_code STRING,
      network STRING,
      patient_status STRING,
      location_id STRING,
      location_city STRING,
      location_county STRING,
      location_region STRING,
      location_timezone STRING,
      call_center_region STRING,
      appointment_status STRING,
      risk_category STRING,
      risk_level STRING,
      cohort STRING,
      sms_consent BOOLEAN,
      is_reminder_eligible BOOLEAN,
      last_pcp_visit_date DATE,
      last_pcp_visit_day_name STRING,
      last_pcp_visit_month_name STRING,
      web_logic_time TIMESTAMP,
      web_submission_time TIMESTAMP,
      approval_time TIMESTAMP,
      submission_day_name STRING,
      submission_month_name STRING,
      submission_quarter INT,
      submission_year INT,
      submission_is_weekend BOOLEAN,
      submission_is_business_day BOOLEAN,
      submission_is_holiday BOOLEAN,
      campaign_week INT,
      days_since_last_pcp_visit INT,
      web_submission_lag_seconds BIGINT,
      approval_lag_seconds BIGINT,
      bronze_layer_timestamp TIMESTAMP,
      silver_layer_timestamp TIMESTAMP,
      gold_layer_timestamp TIMESTAMP,
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
        e.Id,
        p.patient_key,
        l.location_key,
        CAST(date_format(e.web_submission_time, 'yyyyMMdd') AS INT)  AS submission_date_key,
        CAST(date_format(e.last_pcp_visit_date, 'yyyyMMdd') AS INT)  AS last_pcp_visit_date_key,
        e.patient_signature,
        p.patient_id,
        concat_ws(' ', p.first_name, p.last_name)                    AS full_name,
        p.gender,
        p.phone_number,
        p.city                                                       AS patient_city,
        p.state                                                      AS patient_state,
        p.zip_code                                                   AS patient_zip_code,
        p.network,
        p.patient_status,
        l.location_id,
        l.city                                                       AS location_city,
        l.county                                                     AS location_county,
        l.region                                                     AS location_region,
        l.timezone                                                   AS location_timezone,
        l.call_center_region,
        e.appointment_status,
        e.risk_category,
        e.risk_level,
        e.cohort,
        e.sms_consent,
        coalesce(e.sms_consent, false)
          AND e.appointment_status = 'not_scheduled'                 AS is_reminder_eligible,
        e.last_pcp_visit_date,
        dv.day_name                                                  AS last_pcp_visit_day_name,
        dv.month_name                                                AS last_pcp_visit_month_name,
        e.web_logic_time,
        e.web_submission_time,
        e.approval_time,
        ds.day_name                                                  AS submission_day_name,
        ds.month_name                                                AS submission_month_name,
        ds.quarter                                                   AS submission_quarter,
        ds.year                                                      AS submission_year,
        ds.is_weekend                                                AS submission_is_weekend,
        ds.is_business_day                                           AS submission_is_business_day,
        ds.is_holiday                                                AS submission_is_holiday,
        ds.campaign_week,
        datediff(CAST(e.web_submission_time AS DATE), e.last_pcp_visit_date)     AS days_since_last_pcp_visit,
        unix_timestamp(e.web_submission_time) - unix_timestamp(e.web_logic_time) AS web_submission_lag_seconds,
        unix_timestamp(e.approval_time) - unix_timestamp(e.web_submission_time)  AS approval_lag_seconds,
        e.bronze_layer_timestamp,
        e.silver_layer_timestamp,
        current_timestamp()                                          AS gold_layer_timestamp
      FROM {source_table} e
      LEFT JOIN {dim_patients} p
        ON p.patient_signature = e.patient_signature
       AND p.is_active
      LEFT JOIN {dim_location} l
        ON l.zip_code = p.zip_code
       AND l.is_active
      LEFT JOIN {dim_date} ds
        ON ds.date_key = CAST(date_format(e.web_submission_time, 'yyyyMMdd') AS INT)
      LEFT JOIN {dim_date} dv
        ON dv.date_key = CAST(date_format(e.last_pcp_visit_date, 'yyyyMMdd') AS INT)
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
