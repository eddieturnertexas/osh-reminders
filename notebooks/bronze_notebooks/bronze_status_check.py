# Databricks notebook source
# ============================================================================
#    ___  ____  _   _
#   / _ \/ ___|| | | |      OAK STREET HEALTH
#  | | | \___ \| |_| |      Appointment Reminder Pipeline
#  | |_| |___) |  _  |      Databricks Asset Bundle / Medallion
#   \___/|____/|_| |_|
# ============================================================================
#  Notebook : bronze_status_check
#  Author   : eddie turner
#  Date     : 2026-09-13
#  Details  : Rebuilds osh_config_bronze_status from osh_config_bronze_stream.
#             Gives bronze_ingestion one row per stream with a fresh run state.
# ============================================================================
from __future__ import annotations

import re
from pyspark.sql import SparkSession


def get_widget(name: str, default: str) -> str:
    dbutils.widgets.text(name, default)
    value = dbutils.widgets.get(name)
    return value.strip() if value else default


catalog_env = get_widget("catalog_env", "dev")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog_env):
    raise ValueError(f"Invalid catalog_env: {catalog_env!r}")

spark = SparkSession.builder.getOrCreate()
prefix = catalog_env

STATUS_TABLE = f"{prefix}_osh_bronze_db.osh.osh_config_bronze_status"
STREAM_TABLE = f"{prefix}_osh_bronze_db.osh.osh_config_bronze_stream"

spark.sql(
    f"""
    MERGE INTO {STATUS_TABLE} t
    USING (
      SELECT
        bs.StreamId,
        bs.StreamName,
        bs.StreamId AS RunOrder,
        CASE WHEN bs.IsActive = 1 THEN 1 ELSE 0 END AS IsBronzeEnabled,
        0 AS IsBronzeRunning,
        0 AS IsBronzeCompleted,
        CAST(NULL AS TIMESTAMP) AS LastModified
      FROM {STREAM_TABLE} bs
    ) s
    ON t.StreamName = s.StreamName
    WHEN MATCHED THEN
      UPDATE SET
        t.StreamId = s.StreamId,
        t.RunOrder = s.RunOrder,
        t.IsBronzeEnabled = s.IsBronzeEnabled,
        t.IsBronzeRunning = s.IsBronzeRunning,
        t.IsBronzeCompleted = s.IsBronzeCompleted
    WHEN NOT MATCHED THEN
      INSERT (
        StreamId, StreamName, RunOrder, IsBronzeEnabled, IsBronzeRunning, IsBronzeCompleted, LastModified
      )
      VALUES (
        s.StreamId, s.StreamName, s.RunOrder, s.IsBronzeEnabled, s.IsBronzeRunning, s.IsBronzeCompleted, s.LastModified
      )
    """
)

enabled = spark.sql(f"SELECT count(*) AS c FROM {STATUS_TABLE} WHERE IsBronzeEnabled = 1").collect()[0]["c"]
print(f"Synced {STATUS_TABLE} from osh_config_bronze_stream; {enabled} stream(s) enabled for bronze")
