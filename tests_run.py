# Databricks notebook source
import pytest
import sys

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# Run pytest.
retcode = pytest.main([".", "-v", "-p", "no:cacheprovider"])

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."

# COMMAND ----------

pytest.fail

# COMMAND ----------

pytest.exit

# COMMAND ----------

from pyspark.sql.types import DateType
from dq_function import quality_check
from pyspark.sql.functions import col

# parameters
table_name = "raw_document"
db_name = "ac_stg_green"
module = "ti-tables-ticketing"
date_column = "snapshot_date"
airline_code = "AC"

df = spark.sql(f"SELECT * FROM {db_name}.{table_name}") \
    .filter("snapshot_date_time >= '2023-01-01' and snapshot_date_time < '2023-01-08'") \
    .withColumn("snapshot_date", col("snapshot_date_time").cast(DateType()))

results_df = quality_check(df, airline_code, module, table_name, date_column)

# COMMAND ----------


