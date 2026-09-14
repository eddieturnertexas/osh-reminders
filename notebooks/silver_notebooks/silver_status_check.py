# Databricks notebook source
# ============================================================================
#    ___  ____  _   _
#   / _ \/ ___|| | | |      OAK STREET HEALTH
#  | | | \___ \| |_| |      Appointment Reminder Pipeline
#  | |_| |___) |  _  |      Databricks Asset Bundle / Medallion
#   \___/|____/|_| |_|
# ============================================================================
#  Notebook : silver_status_check
#  Author   : eddie turner
#  Date     : 2026-09-13
#  Details  : Rebuilds osh_config_silver_status from osh_config_silver_stream.
#             Gives silver_ingestion one row per stream with a fresh run state.
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

STATUS_TABLE = f"{prefix}_osh_bronze_db.osh.osh_config_silver_status"
STREAM_TABLE = f"{prefix}_osh_bronze_db.osh.osh_config_silver_stream"

spark.sql(
    f"""
    MERGE INTO {STATUS_TABLE} t
    USING (
      SELECT
        bs.StreamId,
        bs.StreamName,
        bs.StreamId AS RunOrder,
        CASE WHEN bs.IsActive = 1 THEN 1 ELSE 0 END AS IsSilverEnabled,
        0 AS IsSilverRunning,
        0 AS IsSilverCompleted,
        CAST(NULL AS TIMESTAMP) AS LastModified
      FROM {STREAM_TABLE} bs
    ) s
    ON t.StreamName = s.StreamName
    WHEN MATCHED THEN
      UPDATE SET
        t.StreamId = s.StreamId,
        t.RunOrder = s.RunOrder,
        t.IsSilverEnabled = s.IsSilverEnabled,
        t.IsSilverRunning = s.IsSilverRunning,
        t.IsSilverCompleted = s.IsSilverCompleted
    WHEN NOT MATCHED THEN
      INSERT (
        StreamId, StreamName, RunOrder, IsSilverEnabled, IsSilverRunning, IsSilverCompleted, LastModified
      )
      VALUES (
        s.StreamId, s.StreamName, s.RunOrder, s.IsSilverEnabled, s.IsSilverRunning, s.IsSilverCompleted, s.LastModified
      )
    """
)

enabled = spark.sql(f"SELECT count(*) AS c FROM {STATUS_TABLE} WHERE IsSilverEnabled = 1").collect()[0]["c"]
print(f"Synced {STATUS_TABLE} from osh_config_silver_stream; {enabled} stream(s) enabled for silver")
