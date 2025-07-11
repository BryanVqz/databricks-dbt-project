# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Building Lakeflow pipeline**

# COMMAND ----------

@dlt.view
def transform_airports():
  df = spark.readStream.format("delta")\
                  .load("/Volumes/workspace_bryanq/bronze/bronze_volume/airports/data/")\
                  .withColumn("modifiedDate", current_timestamp())\
                  .drop('_rescued_data')
  return df

# COMMAND ----------

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
 target="silver_airports",
 source="transform_airports",
 keys=["airport_id"],
 sequence_by = col("modifiedDate"),
 stored_as_scd_type = 1
)
