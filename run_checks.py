# Databricks notebook source
from dq_function import quality_check
from pyspark.sql.functions import *
from pyspark.sql.types import DateType

# COMMAND ----------

# Define parameters
df = spark.sql("select * from ac_stg_green.raw_document") \
    .filter("snapshot_date_time >= '2023-01-01' and snapshot_date_time < '2023-02-01'") \
    .withColumn("snapshot_date", col("snapshot_date_time").cast(DateType()))
date_column = "snapshot_date"
table_name = "raw_document"
airline_code = "AC"
module = "ti-tables-ticketing"

# Run checks


# COMMAND ----------


