# Databricks notebook source
# ============================================================================
#    ___  ____  _   _
#   / _ \/ ___|| | | |      OAK STREET HEALTH
#  | | | \___ \| |_| |      Appointment Reminder Pipeline
#  | |_| |___) |  _  |      Databricks Asset Bundle / Medallion
#   \___/|____/|_| |_|
# ============================================================================
#  Notebook : fill_objects
#  Author   : eddie turner
#  Date     : 2026-09-13
#  Details  : Loads the bronze, silver and gold stream config rows.
#             Each row names the notebook, source and target for one stream.
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

if catalog_env not in allowed_envs:
    raise ValueError(f"Invalid catalog_env: {catalog_env!r}. Expected one of {sorted(allowed_envs)}")

# COMMAND ----------
spark.sql(
    f"""
    INSERT OVERWRITE TABLE {config_schema}.osh_config_bronze_stream (
      StreamId,
      StreamName,
      IsActive,
      ApiAdress,
      ApiToken,
      TargetCatalog,
      TargetSchema,
      TargetTableName,
      TargetSchemaPath,
      TargetCheckpointPath,
      LastModified
    )
    VALUES
      (
        2,
        'osh_event',
        1,
        'osh/osh-reminders',
        'osh/api-token',
        '_osh_bronze_db',
        'osh',
        'osh_event',
        '/Volumes/_osh_bronze_db/osh/osh_ingestion/schemas/osh_event',
        '/Volumes/_osh_bronze_db/osh/osh_ingestion/checkpoints/osh_event',
        TIMESTAMP '2026-09-13T02:28:58.254350Z'
      ),
      (
        3,
        'osh_assist',
        0,
        'osh/osh-reminders',
        'osh/api-token',
        '_osh_bronze_db',
        'osh',
        'osh_assist',
        '/Volumes/_osh_bronze_db/osh/osh_ingestion/schemas/osh_assist',
        '/Volumes/_osh_bronze_db/osh/osh_ingestion/checkpoints/osh_assist',
        TIMESTAMP '2026-09-13T02:28:58.254350Z'
      ),
      (
        4,
        'osh_launcher',
        0,
        'osh/osh-reminders',
        'osh/api-token',
        '_osh_bronze_db',
        'osh',
        'osh_launcher',
        '/Volumes/_osh_bronze_db/osh/osh_ingestion/schemas/osh_launcher',
        '/Volumes/_osh_bronze_db/osh/osh_ingestion/checkpoints/osh_launcher',
        TIMESTAMP '2026-09-13T02:28:58.254350Z'
      )
    """
)

# COMMAND ----------
spark.sql(
    f"""
    INSERT OVERWRITE TABLE {config_schema}.osh_config_silver_stream (
      StreamId,
      StreamName,
      IsActive,
      SourceNotebookPath,
      SourceNotebookName,
      SourceCatalog,
      SourceSchema,
      SourceTableName,
      TargetCatalog,
      TargetSchema,
      TargetTableName,
      SilverStreamCheck,
      LastModified
    )
    VALUES
      (
        2,
        'osh_event',
        1,
        '/Workspace/Repos/osh-reminders/{catalog_env}/files/notebooks/silver_notebooks',
        'silver_osh_event',
        '_osh_bronze_db',
        'osh',
        'osh_event',
        '_osh_silver_db',
        'osh',
        'dim_osh_event',
        '2',
        TIMESTAMP '2026-09-13T02:28:58.254350Z'
      )
    """
)

# COMMAND ----------
spark.sql(
    f"""
    INSERT OVERWRITE TABLE {config_schema}.osh_config_gold_stream (
      StreamId,
      StreamName,
      IsActive,
      SourceNotebookPath,
      SourceNotebookName,
      SourceCatalog,
      SourceSchema,
      SourceTableName,
      TargetCatalog,
      TargetSchema,
      TargetTableName,
      GoldStreamCheck,
      LastModified
    )
    VALUES
      (
        100,
        'osh_reminder_final',
        1,
        '/Workspace/Repos/osh-reminders/{catalog_env}/files/notebooks/gold_notebooks',
        'gold_osh_reminder',
        '_osh_silver_db',
        'osh',
        'dim_osh_event',
        '_osh_gold_db',
        'osh_reminder',
        'fact_osh_reminder',
        '3',
        TIMESTAMP '2026-09-13T02:28:58.254350Z'
      ),
      (
        101,
        'osh_campaign_plan',
        1,
        '/Workspace/Repos/osh-reminders/{catalog_env}/files/notebooks/gold_notebooks',
        'gold_osh_campaign',
        '_osh_silver_db',
        'osh',
        'dim_osh_event',
        '_osh_gold_db',
        'osh_reminder',
        'fact_osh_campaign_plan',
        '3',
        TIMESTAMP '2026-09-13T02:28:58.254350Z'
      )
    """
)

# COMMAND ----------
spark.sql(
    f"""
    INSERT OVERWRITE TABLE {config_schema}.osh_config_bronze_status (
      StreamId,
      StreamName,
      RunOrder,
      IsBronzeEnabled,
      IsBronzeRunning,
      IsBronzeCompleted,
      LastModified
    )
    VALUES
      (
        2,
        'osh_event',
        2,
        1,
        0,
        0,
        NULL
      )
    """
)

print(f"Filled 6 rows across 4 OSH config tables in {config_schema}")
