# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Building Lakeflow pipeline**

# COMMAND ----------

@dlt.view
def stage_bookings():
  df = spark.readStream.format("delta")\
                  .load("/Volumes/workspace_bryanq/bronze/bronze_volume/bookings/data/")
  return df

# COMMAND ----------

@dlt.view
def stageBookingsView():
  df = spark.readStream.table("stage_bookings")
  df = df.withColumn("amount", col("amount").cast(DoubleType()))\
          .withColumn("modifiedDate", current_timestamp())\
          .withColumn("booking_date", to_date(col("booking_date")))\
          .drop('_rescued_data')
  return df

# COMMAND ----------

expectations = {
  "expect_1" : "booking_id IS NOT NULL"
  ,"expect_2" : "passenger_id IS NOT NULL"
}

# COMMAND ----------

@dlt.table
@dlt.expect_all_or_drop(expectations)
def silver_bookings():
  df = spark.readStream.table("stageBookingsView")
  return df
