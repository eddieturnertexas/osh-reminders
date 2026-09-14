# Databricks notebook source
# ============================================================================
#    ___  ____  _   _
#   / _ \/ ___|| | | |      OAK STREET HEALTH
#  | | | \___ \| |_| |      Appointment Reminder Pipeline
#  | |_| |___) |  _  |      Databricks Asset Bundle / Medallion
#   \___/|____/|_| |_|
# ============================================================================
#  Notebook : gold_osh_campaign
#  Author   : eddie turner
#  Date     : 2026-09-13
#  Details  : Ranks the reminder cohort and schedules it across 5 campaign weeks.
#             1000 patients, 200 per week, starting the current week of year.
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

COHORT_SIZE = int(get_widget("cohort_size", "1000"))
CAMPAIGN_WEEKS = int(get_widget("campaign_weeks", "5"))

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
if COHORT_SIZE < 1 or CAMPAIGN_WEEKS < 1:
    raise ValueError("cohort_size and campaign_weeks must both be at least 1.")

WEEKLY_CAPACITY = -(-COHORT_SIZE // CAMPAIGN_WEEKS)

spark = SparkSession.builder.getOrCreate()

dim_namespace = f"{source_table.split('.')[0]}.{dim_schema}"
dim_patients = f"{dim_namespace}.dim_patients"
dim_location = f"{dim_namespace}.dim_location"
dim_date = f"{dim_namespace}.dim_date"

# COMMAND ----------
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {target_table} AS
    WITH ranked AS (
      SELECT
        ROW_NUMBER() OVER (
            ORDER BY CASE c.risk_level
                         WHEN 'Highest' THEN 1
                         WHEN 'High'    THEN 2
                         WHEN 'Medium'  THEN 3
                         ELSE 4
                     END,
                     CASE c.appointment_status
                         WHEN 'not_scheduled' THEN 1
                         WHEN 'cancelled'     THEN 2
                         ELSE 3
                     END,
                     CASE WHEN c.cohort = 'new_vip' THEN 0 ELSE 1 END,
                     c.last_pcp_visit_date,
                     c.Id
        )                                                            AS priority_rank,
        CASE
            WHEN c.risk_level IN ('High','Highest') AND c.appointment_status = 'not_scheduled'
                THEN 'T1 core cohort'
            WHEN c.risk_level IN ('High','Highest') AND c.appointment_status = 'cancelled'
                THEN 'T2 core cohort - cancelled'
            ELSE 'T3 top-up - next risk tier'
        END                                                          AS priority_tier,
        c.*
      FROM (
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
            coalesce(e.sms_consent, false)
              AND e.appointment_status IN ('not_scheduled','cancelled')  AS needs_appointment,
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
      ) c
      WHERE c.sms_consent
        AND c.appointment_status IN ('not_scheduled','cancelled')
        AND c.risk_level IN ('High','Highest','Medium')
        AND c.phone_number IS NOT NULL
      QUALIFY priority_rank <= {COHORT_SIZE}
    ),
    scheduled AS (
      SELECT
        r.*,
        CAST(ceil(r.priority_rank / {WEEKLY_CAPACITY}.0) AS INT) AS send_campaign_week
      FROM ranked r
    )
    SELECT
      s.priority_rank,
      s.priority_tier,
      s.send_campaign_week,
      date_add(current_date(), (s.send_campaign_week - 1) * 7)             AS send_week_start_date,
      weekofyear(date_add(current_date(), (s.send_campaign_week - 1) * 7)) AS send_week_of_year,
      year(date_add(current_date(), (s.send_campaign_week - 1) * 7))       AS send_year,
      weekofyear(current_date())                                           AS campaign_start_week_of_year,
      s.* EXCEPT (priority_rank, priority_tier, send_campaign_week)
    FROM scheduled s
    ORDER BY s.priority_rank
    """
)

# COMMAND ----------
row_count = spark.sql(f"SELECT count(*) AS row_count FROM {target_table}").collect()[0]["row_count"]

spark.sql(
    f"""
    SELECT send_campaign_week, send_week_of_year, send_week_start_date, count(*) AS patients
    FROM {target_table}
    GROUP BY send_campaign_week, send_week_of_year, send_week_start_date
    ORDER BY send_campaign_week
    """
).show(truncate=False)

print(f"{target_table} now holds {row_count} row(s) across {CAMPAIGN_WEEKS} week(s)")
dbutils.notebook.exit(str(row_count))
