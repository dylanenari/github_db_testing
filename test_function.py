from dq_function import *
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType

# initialise Spark
spark = SparkSession.builder \
  .appName('integrity-tests') \
  .getOrCreate()

# parameters
table_name = "raw_document"
db_name = "ac_stg_green"
module = "ti-tables-ticketing"
date_column = "snapshot_date"
airline_code = "AC"
df = spark.sql(f"SELECT * FROM {db_name}.{table_name}") \
    .filter("snapshot_date_time >= '2023-01-01' and snapshot_date_time < '2023-01-08'") \
    .withColumn("snapshot_date", col("snapshot_date_time").cast(DateType()))

# does the table exist?
def test_tableExists():
  assert tableExists(db_name, table_name) is True


# did the checks run?
def test_quality_check():
  results_df = quality_check(df, airline_code, module, table_name, date_column)
  assert not results_df.isEmpty()