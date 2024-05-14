from dq_function import tableExists, quality_check
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.functions import col

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
    .withColumn("snapshot_date", col("snapshot_date_time").cast(DateType()))\
  
results_df = quality_check(df, airline_code, module, table_name, date_column)

# does the table exist?
def test_tableExists():
  assert tableExists(db_name, table_name) is True, "Table not found in database"

# did the checks run?
def test_quality_check():
  assert not results_df.isEmpty(), "Data quality results not generated succesfully"

# does results_df have expected columns?
def test_results_columns():
  expected_columns = set([date_column, "airline_code", "module", "table", "kpi", "key", "value"])
  assert results_df.columns == expected_columns, "Data quality results incomplete"