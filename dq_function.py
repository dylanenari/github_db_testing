from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# initialize Spark
spark = SparkSession.builder \
                    .appName('integrity-tests') \
                    .getOrCreate()

# load dataframe function
def tableExists(table_name, db_name):
  return spark.catalog.tableExists(f"{table_name}.{db_name}")

# data quality checks function
def quality_check(df: DataFrame,
                  airline_code: StringType(),
                  module: StringType(),
                  table_name: StringType(),
                  date_column: StringType(),
                  fk_identifier: StringType() = None):
    from pyspark.sql.types import DateType
    from pyspark.sql.types import StringType
    from pyspark.sql.window import Window
    # ticketing
    if "source" in df.columns:

        ## consistency
        # count rows by feed source and date
        counts_df = df.groupBy(date_column, "source").agg(count("*").alias("value")) \
            .withColumn("module", lit(module)) \
            .withColumn("kpi", lit("row_count")) \
            .withColumn("airline_code", lit(airline_code)) \
            .withColumnRenamed("source", "key") \
            .withColumn("table", lit(table_name)) \
            .union(df.groupBy(date_column).agg(count("*").alias("value")) \
                   .withColumn("module", lit(module)) \
                   .withColumn("kpi", lit("row_count")) \
                   .withColumn("airline_code", lit(airline_code)) \
                   .withColumn("key", lit("general")) \
                   .withColumn("table", lit(table_name)) \
                   .select(date_column, "key", "value", "module", "kpi", "airline_code", "table"))

        ## uniqueness
        # filter duplicate rows
        dupl_df = df.groupBy(df.columns).count().filter("count > 1") \
            .groupBy(date_column, "source").agg(count("*").alias("value")) \
            .withColumn("module", lit(module)) \
            .withColumn("kpi", lit("duplicate_count")) \
            .withColumn("airline_code", lit(airline_code)) \
            .withColumnRenamed("source", "key") \
            .withColumn("table", lit(table_name)) \
            .union(df.groupBy(df.columns).count().filter("count > 1")
                   .groupBy(date_column).agg(count("*").alias("value")) \
                   .withColumn("module", lit(module)) \
                   .withColumn("kpi", lit("duplicate_count")) \
                   .withColumn("airline_code", lit(airline_code)) \
                   .withColumn("key", lit("general")) \
                   .withColumn("table", lit(table_name)) \
                   .select(date_column, "key", "value", "module", "kpi", "airline_code", "table"))

        # reservation
    elif fk_identifier is not None and any(column for column in df.columns if fk_identifier in column):

        fk_cols = [column for column in df.columns if fk_identifier in column]

        ## consistency

        # counts by foreign keys and total
        exprs = [count(c).alias(f"{c}") for c in fk_cols] + [count("*").alias("general")]
        counts_df = df.groupBy(date_column).agg(*exprs)

        # transpose and add columns
        counts_df = counts_df.melt(date_column, [column for column in counts_df.columns if column != date_column],
                                   "key", "value") \
            .withColumn("module", lit(module)) \
            .withColumn("kpi", lit("row_count")) \
            .withColumn("airline_code", lit(airline_code)) \
            .withColumn("table", lit(table_name))

        ## uniqueness
        # filter duplicate rows and apply count expressions
        dupl_df = df.groupBy(df.columns).count().filter("count > 1") \
            .groupBy(date_column).agg(*exprs)

        # transpose and add columns
        dupl_df = dupl_df.melt(date_column, [column for column in dupl_df.columns if column != date_column], "key",
                               "value") \
            .withColumn("module", lit(module)) \
            .withColumn("kpi", lit("duplicate_count")) \
            .withColumn("airline_code", lit(airline_code)) \
            .withColumn("table", lit(table_name))

    # coupons
    else:
        # count by date, add columns
        counts_df = df.groupBy(date_column).agg(count("*").alias("value")) \
            .withColumn("module", lit(module)) \
            .withColumn("kpi", lit("row_count")) \
            .withColumn("airline_code", lit(airline_code)) \
            .withColumn("table", lit(table_name)) \
            .withColumn("key", lit("general")) \
            .select(date_column, "key", "value", "module", "kpi", "airline_code", "table")

        # duplicates by date, add columns
        dupl_df = df.groupBy(df.columns).count().filter("count > 1") \
            .groupBy(date_column).agg(count("*").alias("value")) \
            .withColumn("module", lit(module)) \
            .withColumn("kpi", lit("duplicate_count")) \
            .withColumn("airline_code", lit(airline_code)) \
            .withColumn("table", lit(table_name)) \
            .withColumn("key", lit("general")) \
            .select(date_column, "key", "value", "module", "kpi", "airline_code", "table")

    ### GENERAL PART

    ## completeness
    # define date window
    windowSpec = Window.partitionBy(date_column)
    # loop through columns
    for column in df.columns:
        if column != date_column:
            completeness_col = column + "_non_null_count"
            # count column / count date = completeness over window
            df = df.withColumn(completeness_col, (count(column).over(windowSpec) / count(date_column).over(windowSpec)))

    # drop duplicates
    compl_df = df.select(
        [date_column] + [column for column in df.columns if "_non_null_count" in column]).dropDuplicates()
    # delete _non_null_count from name to get original colu,mn
    for column in compl_df.columns:
        if "_non_null_count" in column:
            compl_df = compl_df.withColumnRenamed(column, column.replace("_non_null_count", ""))

    # melt, add columns
    id_vars = date_column
    values = [col for col in compl_df.columns if col != date_column]
    vbleName = "key"
    vlueName = "value"

    if "source" in df.columns:
        compl_df = compl_df.melt(id_vars, values, vbleName, vlueName) \
            .withColumn("module", lit(module)) \
            .withColumn("kpi", lit("completeness")) \
            .withColumn("airline_code", lit(airline_code)) \
            .withColumn("table", lit(table_name))
    elif fk_identifier is not None and any(column for column in df.columns if fk_identifier in column):
        compl_df = compl_df.melt(id_vars, values, vbleName, vlueName) \
            .withColumn("module", lit(module)) \
            .withColumn("kpi", lit("completeness")) \
            .withColumn("airline_code", lit(airline_code)) \
            .withColumn("table", lit(table_name))
    else:
        compl_df = compl_df.melt(id_vars, values, vbleName, vlueName) \
            .withColumn("module", lit(module)) \
            .withColumn("kpi", lit("completeness")) \
            .withColumn("airline_code", lit(airline_code)) \
            .withColumn("table", lit(table_name))

        # timeliness

    # range of expected dates
    min_date, max_date = df.select(min(date_column), max(date_column)).first()
    date_range_df = spark.createDataFrame([(min_date, max_date)], ["min_date", "max_date"]) \
        .select(explode(expr("sequence(to_date(min_date), to_date(max_date), interval 1 day)")).alias(date_column))

    # dataframe of real dates
    real_dates_df = df.select(date_column).distinct()

    # join to mach dates and union with other kpis

    return counts_df \
        .union(dupl_df) \
        .union(compl_df) \
        .union(real_dates_df.join(date_range_df, date_column) \
               .withColumn("key", lit("general")) \
               .withColumn("value", when(col(date_column).isNull(), "Failure").otherwise("Success")) \
               .withColumn("module", lit(module)) \
               .withColumn("kpi", lit("timeliness")) \
               .withColumn("airline_code", lit(airline_code)) \
               .withColumn("table", lit(table_name))) \
        .select(date_column, "airline_code", "module", "table", "kpi", "key", "value")
