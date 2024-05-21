from src.data_quality_checks import *
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
    .withColumn("snapshot_date", col("snapshot_date_time").cast(DateType()))
  
checks = QualityCheck(spark, df, airline_code, module, table_name, date_column)

# does the table exist?
def test_tableExists():
  assert tableExists(db_name, table_name) is True, "Table not found in database"

# did the row count run?
def test_rows_check():
  counts_df = QualityCheck(spark, df, airline_code, module, table_name, date_column).count_rows()
  assert not counts_df.isEmpty() or counts_df == None, "Row count KPI not generated successfully"

# do row count results have correct schema?
def test_count_cols():
  expected_cols = [date_column, "airline_code", "module", "table", "kpi", "key", "value"]
  counts_df = QualityCheck(spark, df, airline_code, module, table_name, date_column).count_rows()
  assert counts_df.columns == expected_cols, "Wrong schema in row count KPI"

# did the duplicate count run?
def test_duplicates_check():
  dupl_df = QualityCheck(spark, df, airline_code, module, table_name, date_column).count_duplicates()
  assert not dupl_df.isEmpty() or dupl_df == None, "Duplicate count KPI not generated successfully"

# do duplicates count results have correct schema?
def test_duplicates_cols():
  expected_cols = [date_column, "airline_code", "module", "table", "kpi", "key", "value"]
  dupl_df = QualityCheck(spark, df, airline_code, module, table_name, date_column).count_duplicates()
  assert dupl_df.columns == expected_cols, "Wrong schema in duplicate count KPI"

# did the completeness ratio run?
def test_completeness_check():
  compl_df = QualityCheck(spark, df, airline_code, module, table_name, date_column).compute_completeness()
  assert not compl_df.isEmpty() or compl_df == None, "Completeness ratio KPI not generated successfully"

# do completeness ratio results have correct schema?
def test_completeness_cols():
  expected_cols = [date_column, "airline_code", "module", "table", "kpi", "key", "value"]
  compl_df = QualityCheck(spark, df, airline_code, module, table_name, date_column).compute_completeness()
  assert compl_df.columns == expected_cols, "Wrong schema in completeness ratio KPI"

# did the timeliness run?
def test_dates_check():
  dates_df = QualityCheck(spark, df, airline_code, module, table_name, date_column).dates_check()
  assert not dates_df.isEmpty() or dates_df == None, "Date completeness KPI not generated successfully"

# do completeness ratio results have correct schema?
def test_dates_cols():
  expected_cols = [date_column, "airline_code", "module", "table", "kpi", "key", "value"]
  dates_df = QualityCheck(spark, df, airline_code, module, table_name, date_column).dates_check()
  assert dates_df.columns == expected_cols, "Wrong schema in dates completeness KPI"

# did all checks run and combine to produce results dataframe?
def test_generate_results():
  results_df = QualityCheck(spark, df, airline_code, module, table_name, date_column).quality_check()
  assert not results_df.isEmpty() or results_df == None, "DQ results not generated successfully"