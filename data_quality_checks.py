from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# initialize Spark
spark = SparkSession.builder \
                    .appName('integrity-tests') \
                    .getOrCreate()

# load dataframe function
def tableExists(db_name, table_name):
  return spark.catalog.tableExists(f"{db_name}.{table_name}")


class QualityCheck:
    def __init__(self, spark: SparkSession, df: DataFrame,
               airline_code: str, module: str, table_name: str, date_column: str,
               fk_identifier: str = None):
        self.spark = spark
        self.df = df
        self.airline_code = airline_code
        self.module = module
        self.table_name = table_name
        self.date_column = date_column
        self.fk_identifier = fk_identifier

    # Row count
    def count_rows(self):

        # ticketing specific
        if "source" in self.df.columns:

            # count by date, source and date, total and add columns
            counts_df = self.df.groupBy(self.date_column, "source").agg(count("*").alias("value")) \
                .withColumn("module", lit(self.module)) \
                .withColumn("kpi", lit("row_count")) \
                .withColumn("airline_code", lit(self.airline_code)) \
                .withColumnRenamed("source", "key") \
                .withColumn("table", lit(self.table_name)) \
                .union(self.df.groupBy(self.date_column).agg(count("*").alias("value")) \
                    .withColumn("module", lit(self.module)) \
                    .withColumn("kpi", lit("row_count")) \
                    .withColumn("airline_code", lit(self.airline_code)) \
                    .withColumn("key", lit("general")) \
                    .withColumn("table", lit(self.table_name)) \
                    .select(self.date_column, "key", "value", "module", "kpi", "airline_code", "table"))
        
        # reservation specific
        elif self.fk_identifier is not None and any(column for column in self.df.columns if self.fk_identifier in column):
            
            # isolate foreign key columns
            fk_cols = [column for column in self.df.columns if self.fk_identifier in column]

            # counts by date, foreign keys and date, total
            exprs = [count(c).alias(f"{c}") for c in fk_cols] + [count("*").alias("general")]
            counts_df = self.df.groupBy(self.date_column).agg(*exprs)

            # transpose and add columns
            counts_df = counts_df.melt(self.date_column, [column for column in counts_df.columns if column != self.date_column],
                                    "key", "value") \
                .withColumn("module", lit(self.module)) \
                .withColumn("kpi", lit("row_count")) \
                .withColumn("airline_code", lit(self.airline_code)) \
                .withColumn("table", lit(self.table_name))
        
        return counts_df
    
    # Uniqueness
    def count_duplicates(self):
           
        # ticketing specific
        if "source" in self.df.columns:

            # filter duplicate rows, count by date, source and date, total and add columns
            dupl_df = self.df.groupBy(self.df.columns).count().filter("count > 1") \
                .groupBy(self.date_column, "source").agg(count("*").alias("value")) \
                .withColumn("module", lit(self.module)) \
                .withColumn("kpi", lit("duplicate_count")) \
                .withColumn("airline_code", lit(self.airline_code)) \
                .withColumnRenamed("source", "key") \
                .withColumn("table", lit(self.table_name)) \
                .union(self.df.groupBy(self.df.columns).count().filter("count > 1")
                    .groupBy(self.date_column).agg(count("*").alias("value")) \
                    .withColumn("module", lit(self.module)) \
                    .withColumn("kpi", lit("duplicate_count")) \
                    .withColumn("airline_code", lit(self.airline_code)) \
                    .withColumn("key", lit("general")) \
                    .withColumn("table", lit(self.table_name)) \
                    .select(self.date_column, "key", "value", "module", "kpi", "airline_code", "table"))
        
        # reservation specific
        elif self.fk_identifier is not None and any(column for column in self.df.columns if self.fk_identifier in column):
            
            # isolate foreign key columns
            fk_cols = [column for column in self.df.columns if self.fk_identifier in column]            

            # filter duplicate rows and apply count expressions
            exprs = [count(c).alias(f"{c}") for c in fk_cols] + [count("*").alias("general")]
            dupl_df = self.df.groupBy(self.df.columns).count().filter("count > 1") \
                .groupBy(self.date_column).agg(*exprs)

            # transpose and add columns
            dupl_df = dupl_df.melt(self.date_column, [column for column in dupl_df.columns if column != self.date_column], "key", "value") \
                .withColumn("module", lit(self.module)) \
                .withColumn("kpi", lit("duplicate_count")) \
                .withColumn("airline_code", lit(self.airline_code)) \
                .withColumn("table", lit(self.table_name))
        
        # coupons specific
        else:
            # count by date, total, add columns
            counts_df = self.df.groupBy(self.date_column).agg(count("*").alias("value")) \
                .withColumn("module", lit(self.module)) \
                .withColumn("kpi", lit("row_count")) \
                .withColumn("airline_code", lit(self.airline_code)) \
                .withColumn("table", lit(self.table_name)) \
                .withColumn("key", lit("general")) \
                .select(self.date_column, "key", "value", "module", "kpi", "airline_code", "table")           
            
        return dupl_df
    
    # Completeness
    def compute_completeness(self):

        # define date window
        windowSpec = Window.partitionBy(self.date_column)
        # loop through columns
        for column in self.df.columns:
            if column != self.date_column:
                completeness_col = column + "_non_null_count"
                # count column / count date = completeness over window
                compl_df = self.df.withColumn(completeness_col, (count(column).over(windowSpec) / count(self.date_column).over(windowSpec)))

        # drop duplicates
        compl_df = compl_df.select([self.date_column] + [column for column in compl_df.columns if "_non_null_count" in column]).dropDuplicates()
        
        # delete _non_null_count from name to get original colu,mn
        for column in compl_df.columns:
            if "_non_null_count" in column:
                compl_df = compl_df.withColumnRenamed(column, column.replace("_non_null_count", ""))

        # melt, add columns
        id_vars = self.date_column
        values = [column for column in compl_df.columns if column != self.date_column]
        vbleName = "key"
        vlueName = "value"

        # ticketing specific
        if "source" in self.df.columns:
            compl_df = compl_df.melt(id_vars, values, vbleName, vlueName) \
                .withColumn("module", lit(self.module)) \
                .withColumn("kpi", lit("completeness")) \
                .withColumn("airline_code", lit(self.airline_code)) \
                .withColumn("table", lit(self.table_name))
        
        # reservation specific
        elif self.fk_identifier is not None and any(column for column in self.df.columns if self.fk_identifier in column):
            compl_df = compl_df.melt(id_vars, values, vbleName, vlueName) \
                .withColumn("module", lit(self.module)) \
                .withColumn("kpi", lit("completeness")) \
                .withColumn("airline_code", lit(self.airline_code)) \
                .withColumn("table", lit(self.table_name))
        
        # coupons specific
        else:
            compl_df = compl_df.melt(id_vars, values, vbleName, vlueName) \
                .withColumn("module", lit(self.module)) \
                .withColumn("kpi", lit("completeness")) \
                .withColumn("airline_code", lit(self.airline_code)) \
                .withColumn("table", lit(self.table_name))
        
        return compl_df
  
    # Timeliness
    def dates_check(self):

        # range of expected dates
        min_date, max_date = self.df.select(min(self.date_column), max(self.date_column)).first()
        date_range_df = spark.createDataFrame([(min_date, max_date)], ["min_date", "max_date"]) \
            .select(explode(expr("sequence(to_date(min_date), to_date(max_date), interval 1 day)")).alias(self.date_column))

        # dataframe of real dates
        real_dates_df = self.df.select(self.date_column).distinct()

        # join to match real with expected dates
        dates_df = real_dates_df.join(date_range_df, self.date_column) \
                    .withColumn("key", lit("general")) \
                    .withColumn("value", when(col(self.date_column).isNull(), "Failure").otherwise("Success")) \
                    .withColumn("module", lit(self.module)) \
                    .withColumn("kpi", lit("timeliness")) \
                    .withColumn("airline_code", lit(self.airline_code)) \
                    .withColumn("table", lit(self.table_name))
        
        return dates_df