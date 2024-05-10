import pytest
import pyspark
from dq_function import *
from pyspark.sql import SparkSession

# Initialise Spark
spark = SparkSession.builder \
                    .appName('integrity-tests') \
                    .getOrCreate()

# parameters
table_name = "raw_document"
db_name = "ac_stg_green"
module = "ti-tables-ticketing"
date_column = "snapshot_date"
airline_code = "AC"

# Does the table exist?
def test_tableExists():
  assert tableExists(table_name, db_name) is True


# Did the checks run?
def test_quality_check():
    results_df = quality_check(airline_code, module, table_name, date_column)
    assert not results_df.isEmpty()
