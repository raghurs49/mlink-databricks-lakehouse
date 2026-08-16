# Databricks notebook source
# This illustrative notebook consumes synthetic raw JSON records already delivered
# to a Unity Catalog Volume or cloud object storage. Adapt catalog/schema names per environment.

from pyspark.sql import functions as F

CATALOG = "market_data_demo"
SCHEMA = "synthetic_streaming"
RAW_PATH = "/Volumes/market_data_demo/synthetic_streaming/landing/raw"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

bronze = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/bronze_schema")
    .load(RAW_PATH)
    .withColumn("ingested_at", F.current_timestamp())
)

(
    bronze.writeStream
    .option("checkpointLocation", f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/bronze")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.bronze_messages")
)

# COMMAND ----------

data_records = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.bronze_messages")
    .where(F.col("message_type") == "Data")
    .select(
        F.col("payload.family").alias("family"),
        F.col("payload.values").alias("values"),
        "session_id",
        "sequence",
        "received_at",
        "ingested_at",
    )
)

(
    data_records.writeStream
    .option("checkpointLocation", f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/silver")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.silver_records")
)

