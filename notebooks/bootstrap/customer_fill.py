# Databricks notebook source
# ============================================================================
#    ___  ____  _   _
#   / _ \/ ___|| | | |      OAK STREET HEALTH
#  | | | \___ \| |_| |      Appointment Reminder Pipeline
#  | |_| |___) |  _  |      Databricks Asset Bundle / Medallion
#   \___/|____/|_| |_|
# ============================================================================
#  Notebook : customer_fill
#  Author   : eddie turner
#  Date     : 2026-09-13
#  Details  : Seeds dim_patients, dim_location and dim_date in the silver catalog.
#             Patient and location rows come from the CSVs staged in the volume.
# ============================================================================
from pathlib import Path
import re
import yaml


if "spark" not in globals() or "dbutils" not in globals():
    raise RuntimeError("Run this bootstrap in Databricks with Spark and dbutils available.")

notebook_dir = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
config_file = notebook_dir.parents[1] / "config" / "env_variables.yml"
config_variables = yaml.safe_load(config_file.read_text(encoding="utf-8"))["variables"]
bootstrap_settings = config_variables["bootstrap_settings"]["default"]
customer_settings = bootstrap_settings["customer"]
default_catalog_env = config_variables["catalog_env"]["default"]
default_csv_dir = customer_settings["csv_dir"]

if not (default_csv_dir.startswith("/") or re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", default_csv_dir)):
    default_csv_dir = str(notebook_dir / default_csv_dir)

dbutils.widgets.text("catalog_env", default_catalog_env)
dbutils.widgets.text("catalog_name", customer_settings["catalog_name"])
dbutils.widgets.text("schema_name", customer_settings["schema_name"])
dbutils.widgets.text("csv_dir", default_csv_dir)
dbutils.widgets.text("catalog_managed_location", customer_settings["catalog_managed_location"])
dbutils.widgets.text("date_timezone", customer_settings["date_timezone"])
dbutils.widgets.text("campaign_start_date", customer_settings["campaign_start_date"])
dbutils.widgets.dropdown("seed_data", customer_settings["seed_data"], customer_settings["seed_data_choices"])

workspace_host = (
    spark.conf.get("spark.databricks.workspaceUrl").strip().lower().removeprefix("https://").rstrip("/")
)
allowed_envs = bootstrap_settings["allowed_envs"]
requested_catalog_env = (dbutils.widgets.get("catalog_env") or default_catalog_env).strip().lower()
catalog_env = bootstrap_settings["workspace_environments"].get(workspace_host, requested_catalog_env)

if catalog_env not in allowed_envs:
    raise ValueError(f"Invalid catalog_env: {catalog_env!r}. Expected one of {sorted(allowed_envs)}")

catalog_name = dbutils.widgets.get("catalog_name").strip().format(catalog_env=catalog_env)
schema_name = dbutils.widgets.get("schema_name").strip().format(catalog_env=catalog_env)
csv_dir = dbutils.widgets.get("csv_dir").strip().format(catalog_env=catalog_env)
managed_location = dbutils.widgets.get("catalog_managed_location").strip()
date_timezone = dbutils.widgets.get("date_timezone").strip()
campaign_start_date = dbutils.widgets.get("campaign_start_date").strip()
seed_data = dbutils.widgets.get("seed_data") == "true"

TABLES = {
    "dim_patients": {
        "file": customer_settings["seed_files"]["dim_patients"],
        "key": "patient_key",
        "unique": ('patient_key', 'patient_id', 'patient_signature'),
        "ddl": """
            patient_key BIGINT, patient_id STRING, patient_signature STRING,
            member_id STRING, group_id STRING, network STRING, patient_ssn STRING,
            first_name STRING, last_name STRING, date_of_birth DATE, gender STRING,
            phone_number STRING, address_line_1 STRING, address_line_2 STRING,
            city STRING, state STRING, zip_code STRING, country STRING,
            full_address STRING, pcp_id STRING, patient_status STRING,
            source_system STRING, source_record_id STRING, ingestion_id STRING,
            ingested_at TIMESTAMP, processed_at TIMESTAMP, created_at TIMESTAMP,
            updated_at TIMESTAMP, last_modified_at TIMESTAMP, record_hash STRING,
            record_status STRING, is_active BOOLEAN
        """,
    },
    "dim_date": {
        "key": "date_key",
        "unique": ('date_key', 'full_date'),
        "ddl": """
            date_key INT, full_date DATE, day_of_week INT, day_name STRING,
            day_of_month INT, day_of_year INT, week_of_year INT, month_number INT,
            month_name STRING, quarter INT, year INT, is_weekend BOOLEAN,
            is_business_day BOOLEAN, is_holiday BOOLEAN, holiday_name STRING,
            campaign_week INT, source_system STRING, processed_at TIMESTAMP,
            created_at TIMESTAMP, updated_at TIMESTAMP, last_modified_at TIMESTAMP,
            record_status STRING, is_active BOOLEAN
        """,
    },
    "dim_location": {
        "file": customer_settings["seed_files"]["dim_location"],
        "key": "location_key",
        "unique": ('location_key', 'location_id'),
        "ddl": """
            location_key INT, location_id STRING, zip_code STRING, city STRING,
            county STRING, state STRING, region STRING, country STRING,
            timezone STRING, call_center_region STRING, source_system STRING,
            source_record_id STRING, ingestion_id STRING, ingested_at TIMESTAMP,
            processed_at TIMESTAMP, created_at TIMESTAMP, updated_at TIMESTAMP,
            last_modified_at TIMESTAMP, record_hash STRING, record_status STRING,
            is_active BOOLEAN
        """,
    },
}


DATE_SQL = """
WITH settings AS (
    SELECT CAST(from_utc_timestamp(current_timestamp(), :date_timezone) AS DATE) AS today,
           coalesce(try_cast(:campaign_start_date AS DATE),
                    raise_error('Invalid campaign_start_date')) AS campaign_start,
           current_timestamp() AS generated_at
    ), calendar AS (
        SELECT explode(sequence(add_months(today, -:calendar_months_before), add_months(today, :calendar_months_after),
                                INTERVAL 1 DAY)) AS full_date,
               campaign_start, generated_at
        FROM settings
    ), attributes AS (
        SELECT *, weekday(full_date) >= 5 AS is_weekend,
               datediff(full_date, campaign_start) AS campaign_day,
               CASE date_format(full_date, 'MM-dd')
                   WHEN '01-01' THEN concat('New Year', chr(39), 's Day')
                   WHEN '07-04' THEN 'Independence Day'
                   WHEN '12-25' THEN 'Christmas Day'
               END AS holiday_name
        FROM calendar
    )
    SELECT CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
           full_date, weekday(full_date) + 1 AS day_of_week,
           date_format(full_date, 'EEEE') AS day_name,
           dayofmonth(full_date) AS day_of_month, dayofyear(full_date) AS day_of_year,
           weekofyear(full_date) AS week_of_year, month(full_date) AS month_number,
           date_format(full_date, 'MMMM') AS month_name,
           quarter(full_date) AS quarter, year(full_date) AS year,
           is_weekend, NOT is_weekend AND holiday_name IS NULL AS is_business_day,
           holiday_name IS NOT NULL AS is_holiday, holiday_name,
           CASE WHEN campaign_day BETWEEN 0 AND (:campaign_days - 1)
                THEN CAST(floor(campaign_day / 7) + 1 AS INT) END AS campaign_week,
           'SYSTEM_GENERATED' AS source_system,
           generated_at AS processed_at, generated_at AS created_at,
           generated_at AS updated_at, generated_at AS last_modified_at,
           'CURRENT' AS record_status, true AS is_active
    FROM attributes

"""


def quoted_identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("Catalog and schema names must contain letters, digits, or underscores.")
    return f"`{value}`"


def read_seed_frame(spark, csv_dir, spec):
    if csv_dir.startswith(("/Volumes/", "dbfs:/", "file:", "abfss://", "s3://", "gs://")):
        source_path = f"{csv_dir.rstrip('/')}/{spec['file']}"
    else:
        source_path = (Path(csv_dir) / spec["file"]).resolve().as_uri()
    return spark.sql("""
        SELECT * FROM read_files(
            :source_path,
            format => 'csv', schema => :source_schema,
            header => true, enforceSchema => false, mode => 'FAILFAST',
            schemaEvolutionMode => 'none', multiLine => true,
            escape => chr(34), nullValue => '', encoding => 'UTF-8', timeZone => :source_timezone
        )
    """, args={
        "source_path": source_path,
        "source_schema": spec["ddl"],
        "source_timezone": customer_settings["source_timezone"],
    })


def validate_seed_view(spark, view_name, spec):
    checks = ["assert_true(count(*) > 0, 'No seed rows')"]
    for name in spec["unique"]:
        checks.append(
            f"assert_true(count(DISTINCT `{name}`) = count(*), 'Missing or duplicate {name}')"
        )
    checks.append("max(length(to_json(struct(*)))) AS validated_row_width")
    spark.sql(f"SELECT {', '.join(checks)} FROM `{view_name}`").collect()


def bootstrap_dimensions(
    spark, catalog_name, schema_name, csv_dir, managed_location,
    date_timezone, campaign_start_date, seed_data,
):
    catalog = quoted_identifier(catalog_name)
    schema = quoted_identifier(schema_name)
    namespace = f"{catalog}.{schema}"
    spark.conf.set("spark.sql.session.timeZone", customer_settings["session_timezone"])

    create_catalog = f"CREATE CATALOG IF NOT EXISTS {catalog}"
    if managed_location:
        if not managed_location.startswith("abfss://") or "'" in managed_location or "\\" in managed_location:
            raise ValueError("catalog_managed_location must be an abfss:// URI without quotes or backslashes.")
        create_catalog += f" MANAGED LOCATION '{managed_location}'"

    seed_frames = {}
    views = {}
    try:
        if seed_data:
            for table_name, spec in TABLES.items():
                frame = (
                    spark.sql(DATE_SQL, args={
                        "date_timezone": date_timezone,
                        "campaign_start_date": campaign_start_date,
                        "calendar_months_before": customer_settings["calendar_months_before"],
                        "calendar_months_after": customer_settings["calendar_months_after"],
                        "campaign_days": customer_settings["campaign_days"],
                    })
                    if table_name == "dim_date"
                    else read_seed_frame(spark, csv_dir, spec)
                )
                view_name = f"_osh_bootstrap_{table_name}"
                frame.createOrReplaceTempView(view_name)
                views[table_name] = view_name
                validate_seed_view(spark, view_name, spec)
                seed_frames[table_name] = frame

        spark.sql(create_catalog)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
        for table_name, spec in TABLES.items():
            full_name = f"{namespace}.`{table_name}`"
            spark.sql(f"CREATE TABLE IF NOT EXISTS {full_name} ({spec['ddl']}) USING DELTA")
            if seed_data:
                existing_types = {field.name: field.dataType for field in spark.table(full_name).schema}
                source_types = {field.name: field.dataType for field in seed_frames[table_name].schema}
                if existing_types != source_types:
                    raise ValueError(f"{full_name}: existing table schema differs; no seed data was merged.")

        if seed_data:
            for table_name, spec in TABLES.items():
                full_name = f"{namespace}.`{table_name}`"
                matched = "WHEN MATCHED THEN UPDATE SET *" if table_name == "dim_date" else ""
                removed = "WHEN NOT MATCHED BY SOURCE THEN DELETE" if table_name == "dim_date" else ""
                spark.sql(f"""
                    MERGE INTO {full_name} AS target
                    USING `{views[table_name]}` AS source
                    ON target.`{spec['key']}` = source.`{spec['key']}`
                    {matched}
                    WHEN NOT MATCHED THEN INSERT *
                    {removed}
                """)

        count_queries = [
            f"SELECT '{namespace}.`{name}`' AS table_name, count(*) AS row_count "
            f"FROM {namespace}.`{name}`"
            for name in TABLES
        ]
        spark.sql(" UNION ALL ".join(count_queries)).show(truncate=False)
    finally:
        for view_name in views.values():
            spark.catalog.dropTempView(view_name)


if __name__ == "__main__":
    bootstrap_dimensions(
        spark, catalog_name, schema_name, csv_dir, managed_location,
        date_timezone, campaign_start_date, seed_data,
    )
