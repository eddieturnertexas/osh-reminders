# Databricks notebook source
# ============================================================================
#    ___  ____  _   _
#   / _ \/ ___|| | | |      OAK STREET HEALTH
#  | | | \___ \| |_| |      Appointment Reminder Pipeline
#  | |_| |___) |  _  |      Databricks Asset Bundle / Medallion
#   \___/|____/|_| |_|
# ============================================================================
#  Notebook : create_source_objects
#  Author   : eddie turner
#  Date     : 2026-09-13
#  Details  : Creates the OSH catalogs, schemas, ingestion volume and config tables.
#             Everything uses IF NOT EXISTS, so the bootstrap is safe to re-run.
# ============================================================================
from pathlib import Path
import yaml
from pyspark.sql import SparkSession


notebook_dir = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
config_file = notebook_dir.parents[1] / "config" / "env_variables.yml"
config_variables = yaml.safe_load(config_file.read_text(encoding="utf-8"))["variables"]
bootstrap_settings = config_variables["bootstrap_settings"]["default"]
default_catalog_env = config_variables["catalog_env"]["default"]

dbutils.widgets.text("catalog_env", default_catalog_env)

spark = SparkSession.builder.getOrCreate()
workspace_host = (
    spark.conf.get("spark.databricks.workspaceUrl").strip().lower().removeprefix("https://").rstrip("/")
)
allowed_envs = bootstrap_settings["allowed_envs"]
requested_catalog_env = (dbutils.widgets.get("catalog_env") or default_catalog_env).strip().lower()
catalog_env = bootstrap_settings["workspace_environments"].get(workspace_host, requested_catalog_env)
config_schema = bootstrap_settings["config_schema"].format(catalog_env=catalog_env)
ingestion_volume = bootstrap_settings["ingestion_volume"]
silver_schema = bootstrap_settings["silver_schema"].format(catalog_env=catalog_env)
gold_schema = bootstrap_settings["gold_schema"].format(catalog_env=catalog_env)

if catalog_env not in allowed_envs:
    raise ValueError(f"Invalid catalog_env: {catalog_env!r}. Expected one of {sorted(allowed_envs)}")

# COMMAND ----------
target_schemas = [config_schema, silver_schema, gold_schema]

for target_catalog in dict.fromkeys(name.split(".")[0] for name in target_schemas):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")

for target_schema in target_schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {config_schema}.{ingestion_volume}")

# COMMAND ----------
seed_catalog, seed_schema = config_schema.split(".")
seed_dir = f"/Volumes/{seed_catalog}/{seed_schema}/{ingestion_volume}/seed"
seed_files = bootstrap_settings["customer"]["seed_files"].values()

dbutils.fs.mkdirs(seed_dir)
for seed_file in seed_files:
    dbutils.fs.cp(f"file:{notebook_dir}/{seed_file}", f"{seed_dir}/{seed_file}")

print(f"Staged {len(list(seed_files))} seed CSVs into {seed_dir}")

# COMMAND ----------
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {config_schema}.osh_config_bronze_stream (
      StreamId INT,
      StreamName STRING,
      IsActive INT,
      ApiAdress STRING,
      ApiToken STRING,
      TargetCatalog STRING,
      TargetSchema STRING,
      TargetTableName STRING,
      TargetSchemaPath STRING,
      TargetCheckpointPath STRING,
      LastModified TIMESTAMP
    )
    USING DELTA
    """
)

# COMMAND ----------
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {config_schema}.osh_config_silver_stream (
      StreamId INT,
      StreamName STRING,
      IsActive INT,
      SourceNotebookPath STRING,
      SourceNotebookName STRING,
      SourceCatalog STRING,
      SourceSchema STRING,
      SourceTableName STRING,
      TargetCatalog STRING,
      TargetSchema STRING,
      TargetTableName STRING,
      SilverStreamCheck STRING,
      LastModified TIMESTAMP
    )
    USING DELTA
    """
)

# COMMAND ----------
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {config_schema}.osh_config_gold_stream (
      StreamId INT,
      StreamName STRING,
      IsActive INT,
      SourceNotebookPath STRING,
      SourceNotebookName STRING,
      SourceCatalog STRING,
      SourceSchema STRING,
      SourceTableName STRING,
      TargetCatalog STRING,
      TargetSchema STRING,
      TargetTableName STRING,
      GoldStreamCheck STRING,
      LastModified TIMESTAMP
    )
    USING DELTA
    """
)

# COMMAND ----------
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {config_schema}.osh_config_bronze_status (
      StreamId INT,
      StreamName STRING NOT NULL,
      RunOrder INT,
      IsBronzeEnabled INT,
      IsBronzeRunning INT,
      IsBronzeCompleted INT,
      LastModified TIMESTAMP
    )
    USING DELTA
    """
)

# COMMAND ----------
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {config_schema}.osh_config_silver_status (
      StreamId INT,
      StreamName STRING NOT NULL,
      RunOrder INT,
      IsSilverEnabled INT,
      IsSilverRunning INT,
      IsSilverCompleted INT,
      LastModified TIMESTAMP
    )
    USING DELTA
    """
)

# COMMAND ----------
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {config_schema}.osh_config_gold_status (
      StreamId INT,
      StreamName STRING NOT NULL,
      RunOrder INT,
      IsGoldEnabled INT,
      IsGoldRunning INT,
      IsGoldCompleted INT,
      LastModified TIMESTAMP
    )
    USING DELTA
    """
)

# COMMAND ----------
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {config_schema}.osh_config_log_header (
      DbxJobId STRING,
      DbxRunId STRING,
      JobName STRING,
      PipelineName STRING,
      TaskName STRING,
      TaskRunId STRING,
      Status STRING,
      UserName STRING,
      EventTimestamp TIMESTAMP
    )
    USING DELTA
    """
)

# COMMAND ----------
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {config_schema}.osh_config_log_header_detail (
      StreamName STRING,
      PipelineName STRING,
      TaskName STRING,
      TaskRunId STRING,
      EventType STRING,
      Status STRING,
      BatchId BIGINT,
      RowCount BIGINT,
      FileCount BIGINT,
      JobId STRING,
      RunId STRING,
      Message STRING,
      EventTimestamp TIMESTAMP
    )
    USING DELTA
    """
)

print(f"Created 8 OSH config tables in {config_schema}")
