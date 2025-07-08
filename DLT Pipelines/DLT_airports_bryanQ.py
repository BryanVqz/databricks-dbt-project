# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Building Lakeflow pipeline**

# COMMAND ----------

@dlt.view
def stage_airports():
  df = spark.readStream.format("delta")\
                  .load("/Volumes/workspace_bryanq/bronze/bronze_volume/airports/data/")
  return df

# COMMAND ----------

@dlt.view
def stageAirportsView():
  df = spark.readStream.table("stage_airports")
  df = df.withColumn("modifiedDate", current_timestamp())\
          .drop('_rescued_data')
  return df

# COMMAND ----------

expectations = {
  "expect_1" : "airport_id IS NOT NULL"
  ,"expect_2" : "airport_name IS NOT NULL"
}

# COMMAND ----------

@dlt.table
@dlt.expect_all_or_drop(expectations)
def silver_airports():
  df = spark.readStream.table("stageAirportsView")
  return df
