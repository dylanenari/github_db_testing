from dataclasses import dataclass
from functools import reduce as reduce_function

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

    @property
    def segmentation_keys(self):
        segmentation_keys = []
        if 'source' in self.df.columns:
            segmentation_keys.append('source')
        if self.fk_identifier:
            segmentation_keys.extend(df_col for df_col in self.df.columns if self.fk_identifier in df_col)

        return segmentation_keys

    # row count
    def count_rows(self):

        # count by date, total, add columns
        counts_df = self.df.groupBy(self.date_column).agg(count("*").alias("value")) \
            .withColumn("key", lit(None)) \
            .select(self.date_column, "key", "value")
            
        segmented_df = None

        if self.segmentation_keys:

            partial_kpis = (self.df.groupBy(self.date_column, c)
                                            .agg(count(c).alias("value"))
                                            .withColumnRenamed(c, "key")
                                            .select(self.date_column, "key", "value")
                                            for c in self.segmentation_keys
                            )
            
            segmented_df = reduce_function(lambda df1, df2: df1.union(df2), partial_kpis)

            # Add segmented data to general data
            counts_df = counts_df.union(segmented_df)

        return self._append_metadata(counts_df, "row_count")

    # uniqueness
    def count_duplicates(self):

        # Counting row repetition
        working_df = self.df.groupBy(self.df.columns).count().filter("count > 1")

        dupl_df = working_df.groupBy(self.date_column).agg(sum("count").alias("value")) \
            .withColumn("key", lit(None)) \
            .select(self.date_column, "key", "value")

        segmented_df = None

        # ticketing specific
        if self.segmentation_keys:
            # filter duplicate rows and apply count expressions
            working_df.cache()
            partial_kpis = (working_df.groupBy(self.date_column, c)
                            .agg(sum("count").alias("value"))
                            .withColumnRenamed(c, "key")
                            .select(self.date_column, "key", "value")
                            for c in self.segmentation_keys)

            # use functools.reduce to combine the DataFrames
            segmented_df = reduce_function(lambda df1, df2: df1.union(df2), partial_kpis)

            dupl_df = dupl_df.union(segmented_df)

        return self._append_metadata(dupl_df, "duplicate_count")
            #segmented_df = reduce(lambda df1, df2: df1.union(df2), partial_kpis)

            #dupl_df = dupl_df.union(segmented_df)

        #return self._append_metadata(dupl_df, "duplicate_count")

    # completeness
    def compute_completeness(self):
        
        ratio_cols = [(count(col) / count(self.date_column)).alias(col)
                for col in self.df.columns if col != self.date_column]
        
        completeness_df = (self.df.groupby(self.date_column).agg(*ratio_cols)
                           .melt(ids=self.date_column, values=None,
                                 variableColumnName="key", valueColumnName="value"))

        return self._append_metadata(completeness_df, "completeness")

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

        return reduce_function(lambda df1, df2: df1.union(df2), checks)
