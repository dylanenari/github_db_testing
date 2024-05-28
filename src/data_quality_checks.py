from dataclasses import dataclass
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# initialize Spark
spark = SparkSession.builder \
    .appName('integrity-tests') \
    .getOrCreate()


# load dataframe function
def table_exists(db_name, table_name):
    return spark.catalog.tableExists(f"{db_name}.{table_name}")


# define quality check class
@dataclass
class QualityCheck:
    ss: SparkSession
    df: DataFrame
    airline_code: str
    module: str
    table_name: str
    date_column: str
    fk_identifier: str = None

    def _append_metadata(self, df, method):
        return df.withColumn("module", lit(self.module)) \
            .withColumn("kpi", lit(method)) \
            .withColumn("airline_code", lit(self.airline_code)) \
            .withColumn("table", lit(self.table_name))

    # row count
    def count_rows(self):

        # count by date, total, add columns
        counts_df = self.df.groupBy(self.date_column).agg(count("*").alias("value")) \
            .withColumn("key", lit(None))
        segmented_df = None

        # ticketing specific
        if "source" in self.df.columns:

            # count by date, source and date, total and add columns
            segmented_df = self.df.groupBy(self.date_column, "source").agg(count("*").alias("value")) \
                .withColumnRenamed("source", "key")

        # reservation specific
        elif self.fk_identifier is not None and any(
                df_col for df_col in self.df.columns if self.fk_identifier in df_col):

            # isolate foreign key columns
            fk_cols = [df_col for df_col in self.df.columns if self.fk_identifier in df_col]

            # counts by date, foreign keys and date, total
            exprs = [count(c).alias(f"{c}") for c in fk_cols]
            segmented_df = self.df.groupBy(self.date_column).agg(*exprs)

            # transpose and add columns
            segmented_df = segmented_df.melt(self.date_column, [df_col for df_col in counts_df.columns
                                                                if df_col != self.date_column],
                                             "key", "value")

        if segmented_df is not None:
            counts_df = counts_df.union(segmented_df)

        return self._append_metadata(counts_df, "row_count")

    # uniqueness
    def count_duplicates(self):

        # Counting row repetition
        working_df = self.df.groupBy(self.df.columns).count().filter("count > 1")

        dupl_df = working_df.groupBy(self.date_column).agg(sum("count").alias("value")) \
            .withColumn("key", lit(None))
        segmented_df = None

        # ticketing specific
        if "source" in self.df.columns:
            working_df.cache()
            # filter duplicate rows, count by date, source and date, total and add columns
            segmented_df = working_df.groupBy(self.date_column, "source").agg(sum("count").alias("value")) \
                .withColumnRenamed("source", "key")

        # reservation specific
        elif self.fk_identifier is not None and any(
                df_col for df_col in self.df.columns if self.fk_identifier in df_col):

            # isolate foreign key columns
            fk_cols = [df_col for df_col in self.df.columns if self.fk_identifier in df_col]

            # filter duplicate rows and apply count expressions
            partial_kpis = (working_df.groupBy(self.date_column, c)
                            .agg(sum("count").alias("value"))
                            .withColumnRenamed(c, "key")
                            for c in fk_cols)
            working_df.cache()
            segmented_df = reduce(lambda df1, df2: df1.union(df2), partial_kpis)

        if segmented_df is not None:
            dupl_df = dupl_df.union(segmented_df)

        return self._append_metadata(dupl_df, "duplicate_count")

    # completeness
    def compute_completeness(self):

        # define date window
        window_spec = Window.partitionBy(self.date_column)
        # loop through columns
        for df_col in self.df.columns:
            if df_col != self.date_column:
                completeness_col = df_col + "_non_null_count"
                # count column / count date = completeness over window
                self.df = self.df.withColumn(completeness_col, (
                        count(df_col).over(window_spec) / count(self.date_column).over(window_spec)))

        # drop duplicates
        compl_df = self.df.select(
            [self.date_column] + [df_col for df_col in self.df.columns if "_non_null_count" in df_col]).dropDuplicates()

        # delete _non_null_count from name to get original column
        for df_col in compl_df.columns:
            if "_non_null_count" in df_col:
                compl_df = compl_df.withColumnRenamed(df_col, df_col.replace("_non_null_count", ""))

        # melt, add columns
        id_vars = self.date_column
        values = [df_col for df_col in compl_df.columns if df_col != self.date_column]
        vble_name = "key"
        vlue_name = "value"

        # ticketing specific
        if "source" in self.df.columns:
            compl_df = compl_df.melt(id_vars, values, vble_name, vlue_name) \
                .select(self.date_column, "key", "value")

        # reservation specific
        elif self.fk_identifier is not None and any(
                df_col for df_col in self.df.columns if self.fk_identifier in df_col):
            compl_df = compl_df.melt(id_vars, values, vble_name, vlue_name) \
                .select(self.date_column, "key", "value")

        # coupons specific
        else:
            compl_df = compl_df.melt(id_vars, values, vble_name, vlue_name) \
                .select(self.date_column, "key", "value")

        return self._append_metadata(compl_df, "completeness")

    # timeliness
    def dates_check(self):

        # range of expected dates
        min_date, max_date = self.df.select(min(self.date_column), max(self.date_column)).first()
        date_series = expr("sequence(to_date(min_date), to_date(max_date), interval 1 day)")
        date_range_df = spark.createDataFrame([(min_date, max_date)], ["min_date", "max_date"]) \
            .select(explode(date_series).alias(self.date_column))

        # dataframe of real dates
        real_dates_df = self.df.select(self.date_column).distinct()

        # join to match real with expected dates
        is_there_data = when(col(self.date_column).isNull(), "Failure").otherwise("Success")
        dates_df = real_dates_df.join(date_range_df, self.date_column) \
            .select(self.date_column, lit(None).alias("key"), is_there_data.alias("value"))

        return self._append_metadata(dates_df, "timeliness")

    # all kpis
    def quality_check(self):

        # call all functions and union
        checks = [
            self.count_rows(),
            self.count_duplicates(),
            self.compute_completeness(),
            self.dates_check()
        ]

        return reduce(lambda df1, df2: df1.union(df2), checks)
