# Databricks notebook source
# MAGIC %md
# MAGIC ### Generate results

# COMMAND ----------

from src.data_quality_checks import *
import pyspark.sql.functions as sf 
from pyspark.sql.types import StringType, DateType

# parameters
table_name = "raw_document"
db_name = "ac_stg_green"
module = "ti-tables-ticketing"
date_column = "snapshot_date"
airline_code = "AC"

# if table exists in database:
if table_exists(db_name, table_name):

    df = spark.sql(f"SELECT * FROM {db_name}.{table_name}") \
        .filter("snapshot_date_time >= '2023-01-01' and snapshot_date_time < '2023-06-01'") \
        .withColumn("snapshot_date", col("snapshot_date_time").cast(DateType()))
    
    # run checks
    results_df = QualityCheck(spark, df, airline_code, module, table_name, date_column).quality_check()  

    #if results_df.isEmpty():
        #print(f"The data quality checks were not run succesfully.")
    #else:
        #display(results_df)    

else:
    print(f"The especified table '{table_name}' does not exist in database '{db_name}'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plot, analyze distribution

# COMMAND ----------

from pyspark.sql.types import IntegerType

# Filter series, cast numeric
general_count_df = results_df.filter((sf.col("kpi") == "row_count") & sf.col("key").isNull()) \
    .withColumn("value", sf.col("value").cast(IntegerType()))

# COMMAND ----------

import matplotlib.pyplot as plt
import scipy.stats as stats
import numpy as np

# COMMAND ----------

general_count_df_pd = general_count_df.to_pandas_on_spark().sort_values(by="snapshot_date")

fig, axs = plt.subplots(2, 2, figsize=(12, 8))

# Evolution plot
axs[0, 0].plot(general_count_df_pd["snapshot_date"], general_count_df_pd["value"])
axs[0, 0].set_title("Evolution plot")

# Histogram
axs[0, 1].hist(general_count_df_pd["value"], bins="auto")
axs[0, 1].set_title("Histogram with 'auto' bins")

# QQ plot
stats.probplot(general_count_df_pd["value"].to_numpy(), plot=axs[1, 0])
axs[1, 0].set_title("QQ plot")

# Adjust layout
plt.tight_layout()

# Shapiro-Wilk Test
from scipy.stats import shapiro
stat, p = shapiro(general_count_df_pd["value"].to_numpy())
print("------------------------")
if p > 0.05:
    print(f"S-P Test: p-value = {p} > 0.05 => Distrbution IS normal at 5% significance level")
else:
    print(f"S-P Test: p-value = {p} < 0.05 => Distrbution NOT normal at 5% significance level")
print("------------------------")


# COMMAND ----------

# MAGIC %md
# MAGIC ### IQR
# MAGIC Define anomalies as values outside of IQR

# COMMAND ----------

# Calculate IQR bounds
Q1, Q3 = general_count_df.stat.approxQuantile("value", [0.25, 0.75], 0)
IQR = Q3 -Q1

lower = Q1 - 1.5 * (IQR)
upper = Q3 + 1.5 * (IQR)

# Create the anomalies column
general_count_iqr = general_count_df.withColumn("is_anomaly", sf.when((sf.col("value") < lower) | (sf.col("value") > upper), 1).otherwise(0))

# COMMAND ----------

# Function to visualize results
def visualize_results(df):
    fig, axs = plt.subplots(1, 2, figsize=(15, 5))
    anomalies = df[df["is_anomaly"] == 1]

    axs[0].scatter(df["snapshot_date"].to_numpy(), df["value"].to_numpy(), label='Value')
    axs[0].scatter(anomalies["snapshot_date"].to_numpy(), anomalies["value"].to_numpy(), color='red', marker='.', s=100, label='Anomaly')
    axs[0].set_xlabel("Snapshot Date")
    axs[0].set_ylabel("Value")
    axs[0].legend()

    axs[1].plot(df["snapshot_date"].to_numpy(), df["value"].to_numpy(), label='Value')
    axs[1].scatter(anomalies["snapshot_date"].to_numpy(), anomalies["value"].to_numpy(), color='red', marker='.', s=100, label='Anomaly')
    axs[1].set_xlabel("Snapshot Date")
    axs[1].set_ylabel("Value")
    axs[1].legend()

    plt.tight_layout()
    plt.show()

# COMMAND ----------

# Plot results
df_iqr = general_count_iqr.to_pandas_on_spark().sort_values(by="snapshot_date")

visualize_results(df_iqr)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Z-score
# MAGIC Define anomalies as z-score > 3. If distribution is perfectly normal, 99.7% of points are within 3 standards deviations from the mean.

# COMMAND ----------

# Calculate mean and standard deviation
std = general_count_df.agg(sf.stddev("value")).first()[0]
mean = general_count_df.agg(sf.mean("value")).first()[0]

# Create anomalies column with threshold of 3* z-score
general_count_z = general_count_df.withColumn("is_anomaly", sf.when((abs(col("value") - mean) / std) > 3, 1).otherwise(0))

# COMMAND ----------

# Plot results
df_z = general_count_z.to_pandas_on_spark().sort_values(by="snapshot_date")

visualize_results(df_z)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Implement with UDF to reutilise for each series in results

# COMMAND ----------

# For z-score method
def std_anomaly(value):
    return 1 if abs((value - mean) / std) > 3 else 0
std_anomaly_udf = sf.udf(std_anomaly, IntegerType())

mean = general_count_df.agg(sf.mean(col("value"))).first()[0]
std = general_count_df.agg(sf.stddev(col("value"))).first()[0]

general_count_z_udf = general_count_df.withColumn("is_anomaly", std_anomaly_udf(sf.col("value")))

# COMMAND ----------

# For IQR method
def iqr_anomaly(value):
    return 1 if value < lower or value > upper else 0
iqr_anomaly_udf = sf.udf(iqr_anomaly, IntegerType())

# Calculate IQR bounds
Q1, Q3 = general_count_df.stat.approxQuantile("value", [0.25, 0.75], 0)
IQR = Q3 -Q1

lower = Q1 - 1.5 * (IQR)
upper = Q3 + 1.5 * (IQR)

general_count_iqr_udf = general_count_df.withColumn("is_anomaly", iqr_anomaly_udf(sf.col("value")))

# COMMAND ----------

# Calculate mean and standard deviation
stats = general_count_df.select(sf.mean("value").alias("mean"), sf.stddev("value").alias("stddev")).collect()
mean = stats[0]["mean"]
stddev = stats[0]["stddev"]

def std_anomaly(value):
    return 1 if abs((value - mean) / std) > 3 else 0
std_anomaly_udf = sf.udf(std_anomaly, IntegerType())

# Define the UDF
def z_score_anomaly(value):
    return 1 if abs((value - mean) / stddev) > 3 else 0

z_score_anomaly_udf = udf(z_score_anomaly, IntegerType())

# Add the new column using the UDF
df_with_anomaly = general_count_df.withColumn("is_anomaly", z_score_anomaly_udf(sf.col("value")))

# Show the result
df_with_anomaly.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### IsolationForest

# COMMAND ----------

# MAGIC %md
# MAGIC #### Imports, dependencies, installations

# COMMAND ----------

df = general_count_df.toPandas().sort_values(by="snapshot_date")

# COMMAND ----------

from pyod.models.iforest import IForest

y = df['value'].values.reshape(-1,1)

clf = IForest()
clf.fit(y)

# Get the prediction labels
y_train_pred = clf.labels_ 

# Outlier scores
y_train_scores = clf.decision_scores_

# Add columns to dataframe
df_if = df.copy()
df_if['outlier_scores'] = y_train_scores
df_if['is_anomaly'] = y_train_pred

# COMMAND ----------

visualize_results(df_if)

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNN

# COMMAND ----------

from pyod.models.knn import KNN

y = df['value'].values.reshape(-1,1)

clf = KNN(method='largest')
clf.fit(y)

# Get the prediction labels
y_train_pred = clf.labels_ 

# Outlier scores
y_train_scores = clf.decision_scores_

# Add columns to dataframe
df_knn = df.copy()
df_knn['outlier_scores'] = y_train_scores
df_knn['is_anomaly'] = y_train_pred

# COMMAND ----------

visualize_results(df_knn)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ECOD

# COMMAND ----------

from pyod.models.ecod import ECOD

y = df['value'].values.reshape(-1,1)

clf = ECOD()
clf.fit(y)

# Get the prediction labels
y_train_pred = clf.labels_ 

# Outlier scores
y_train_scores = clf.decision_scores_

# Add columns to dataframe
df_ecod = df.copy()
df_ecod['outlier_scores'] = y_train_scores
df_ecod['is_anomaly'] = y_train_pred

# Visualize
visualize_results(df_ecod)

# COMMAND ----------

df_ecod.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SUOD

# COMMAND ----------

# MAGIC %pip install suod

# COMMAND ----------

from pyod.models.suod import SUOD

y = df['value'].values.reshape(-1,1)

clf = ECOD()
clf.fit(y)

# Get the prediction labels
y_train_pred = clf.labels_ 

# Outlier scores
y_train_scores = clf.decision_scores_

# Add columns to dataframe
df_suod = df.copy()
df_suod['outlier_scores'] = y_train_scores
df_suod['is_anomaly'] = y_train_pred

# Visualize
visualize_results(df_suod)

# COMMAND ----------

# MAGIC %md
# MAGIC ### All

# COMMAND ----------

dfs = [df_iqr, df_z, df_if, df_knn, df_ecod, df_suod]
names = ['IQR', 'Z-Score', 'IsolationForest', 'KNN', 'ECOD', 'SUOD']

fig, axs = plt.subplots(2, 3, figsize=(15, 8))

axs = axs.flatten()

for i, (df, name) in enumerate(zip(dfs, names)):
    anomalies = df[df["is_anomaly"] == 1]
    
    axs[i].scatter(df["snapshot_date"].to_numpy(), df["value"].to_numpy(), label='Value')
    axs[i].scatter(anomalies["snapshot_date"].to_numpy(), anomalies["value"].to_numpy(), color='red', marker='.', s=100, label='Anomaly')
    axs[i].set_xlabel("Snapshot Date")
    axs[i].set_ylabel("Value")
    axs[i].set_title(name)
    axs[i].legend()

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multivariate

# COMMAND ----------

# MAGIC %md
# MAGIC #### Isolate individual series:
# MAGIC - Row_count: general + each feed source
# MAGIC - Duplicates: general + each feed source

# COMMAND ----------

results = results_df.toPandas()

# COMMAND ----------

results.sort_values(by='snapshot_date', inplace=True)

# COMMAND ----------

results.head()

# COMMAND ----------

results = results.loc[(results['kpi'] != 'timeliness') & (results['kpi'] != 'completeness')]
results['value'] = results['value'].astype(float)

# COMMAND ----------

import pandas as pd

# Row counts
general_rows = results.loc[(results['kpi'] == 'row_count') & (results['key'].isna()), ['snapshot_date', 'value']]
tcn_rows = results.loc[(results['kpi'] == 'row_count') & (results['key'] == 'ETS-TCN'), ['snapshot_date', 'value']]
lft_rows = results.loc[(results['kpi'] == 'row_count') & (results['key'] == 'ETS-LFT'), ['snapshot_date', 'value']]
prorating_rows = results.loc[(results['kpi'] == 'row_count') & (results['key'] == 'TSR-PRORATING'), ['snapshot_date', 'value']]
ret_rows = results.loc[(results['kpi'] == 'row_count') & (results['key'] == 'TSR-RET'), ['snapshot_date', 'value']]
eml_rows = results.loc[(results['kpi'] == 'row_count') & (results['key'] == 'ETS-EML'), ['snapshot_date', 'value']]
hot_rows = results.loc[(results['kpi'] == 'row_count') & (results['key'] == 'TSR-HOT'), ['snapshot_date', 'value']]
sprf_rows = results.loc[(results['kpi'] == 'row_count') & (results['key'] == 'TSR-SPRF'), ['snapshot_date', 'value']]

# Duplicates
general_dupl = results.loc[(results['kpi'] == 'duplicate_count') & (results['key'].isna()), ['snapshot_date', 'value']]
tcn_dupl = results.loc[(results['kpi'] == 'duplicate_count') & (results['key'] == 'ETS-TCN'), ['snapshot_date', 'value']]
lft_dupl = results.loc[(results['kpi'] == 'duplicate_count') & (results['key'] == 'ETS-LFT'), ['snapshot_date', 'value']]
prorating_dupl = results.loc[(results['kpi'] == 'duplicate_count') & (results['key'] == 'TSR-PRORATING'), ['snapshot_date', 'value']]
ret_dupl = results.loc[(results['kpi'] == 'duplicate_count') & (results['key'] == 'TSR-RET'), ['snapshot_date', 'value']]
eml_dupl = results.loc[(results['kpi'] == 'duplicate_count') & (results['key'] == 'ETS-EML'), ['snapshot_date', 'value']]
hot_dupl = results.loc[(results['kpi'] == 'duplicate_count') & (results['key'] == 'TSR-HOT'), ['snapshot_date', 'value']]
sprf_dupl = results.loc[(results['kpi'] == 'duplicate_count') & (results['key'] == 'TSR-SPRF'), ['snapshot_date', 'value']]

# Rename the 'value' column to correspond to each series
general_rows.rename(columns={'value': 'general_rows'}, inplace=True)
tcn_rows.rename(columns={'value': 'tcn_rows'}, inplace=True)
lft_rows.rename(columns={'value': 'lft_rows'}, inplace=True)
prorating_rows.rename(columns={'value': 'prorating_rows'}, inplace=True)
ret_rows.rename(columns={'value': 'ret_rows'}, inplace=True)
eml_rows.rename(columns={'value': 'eml_rows'}, inplace=True)
hot_rows.rename(columns={'value': 'hot_rows'}, inplace=True)
sprf_rows.rename(columns={'value': 'sprf_rows'}, inplace=True)
general_dupl.rename(columns={'value': 'general_dupl'}, inplace=True)
tcn_dupl.rename(columns={'value': 'tcn_dupl'}, inplace=True)
lft_dupl.rename(columns={'value': 'lft_dupl'}, inplace=True)
prorating_dupl.rename(columns={'value': 'prorating_dupl'}, inplace=True)
ret_dupl.rename(columns={'value': 'ret_dupl'}, inplace=True)
eml_dupl.rename(columns={'value': 'eml_dupl'}, inplace=True)
hot_dupl.rename(columns={'value': 'hot_dupl'}, inplace=True)
sprf_dupl.rename(columns={'value': 'sprf_dupl'}, inplace=True)

# Merge all dataframes on 'snapshot_date' using outer join
combined_df = general_rows
for df in [tcn_rows, lft_rows, prorating_rows, ret_rows, eml_rows, hot_rows, sprf_rows, general_dupl, tcn_dupl, lft_dupl, prorating_dupl, ret_dupl, eml_dupl, hot_dupl, sprf_dupl]:
    combined_df = pd.merge(combined_df, df, on='snapshot_date', how='outer')

combined_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fit ECOD model on aggregate series

# COMMAND ----------

from sklearn.preprocessing import StandardScaler
from pyod.models.ecod import ECOD

X = combined_df.drop(columns=['snapshot_date'])  # Drop snapshot_date as a feature
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Fit the ECOD model
clf = ECOD()
clf.fit(X_scaled)

# Predict the outliers
outliers = clf.labels_
outlier_scores = clf.decision_scores_

# Add the results to the original DataFrame
combined_df['outlier'] = outliers
combined_df['outlier_score'] = outlier_scores

# COMMAND ----------

combined_df.head(100)

# COMMAND ----------

outliers = combined_df[combined_df["outlier"] == 1]

plt.scatter(combined_df["snapshot_date"].to_numpy(), combined_df["general_rows"].to_numpy(), label='Value')
plt.scatter(outliers["snapshot_date"].to_numpy(), outliers["general_rows"].to_numpy(), color='red', marker='.', s=100, label='Anomaly')
plt.set_xlabel("Snapshot Date")
plt.set_ylabel("Value")
plt.legend()

# COMMAND ----------

# MAGIC %md
# MAGIC Not great results...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Univariate on each series

# COMMAND ----------

# Row counts
general_rows = results.loc[(results['kpi'] == 'row_count') & (results['key'].isna())]
tcn_rows = results.loc[(results['kpi'] == 'row_count') & (results['key'] == 'ETS-TCN')]
lft_rows = results.loc[(results['kpi'] == 'row_count') & (results['key'] == 'ETS-LFT')]
prorating_rows = results.loc[(results['kpi'] == 'row_count') & (results['key'] == 'TSR-PRORATING')]
ret_rows = results.loc[(results['kpi'] == 'row_count') & (results['key'] == 'TSR-RET')]
eml_rows = results.loc[(results['kpi'] == 'row_count') & (results['key'] == 'ETS-EML')]
hot_rows = results.loc[(results['kpi'] == 'row_count') & (results['key'] == 'TSR-HOT')]
sprf_rows = results.loc[(results['kpi'] == 'row_count') & (results['key'] == 'TSR-SPRF')]

# Duplicates
general_dupl = results.loc[(results['kpi'] == 'duplicate_count') & (results['key'].isna())]
tcn_dupl = results.loc[(results['kpi'] == 'duplicate_count') & (results['key'] == 'ETS-TCN')]
lft_dupl = results.loc[(results['kpi'] == 'duplicate_count') & (results['key'] == 'ETS-LFT')]
prorating_dupl = results.loc[(results['kpi'] == 'duplicate_count') & (results['key'] == 'TSR-PRORATING')]
ret_dupl = results.loc[(results['kpi'] == 'duplicate_count') & (results['key'] == 'TSR-RET')]
eml_dupl = results.loc[(results['kpi'] == 'duplicate_count') & (results['key'] == 'ETS-EML')]
hot_dupl = results.loc[(results['kpi'] == 'duplicate_count') & (results['key'] == 'TSR-HOT')]
sprf_dupl = results.loc[(results['kpi'] == 'duplicate_count') & (results['key'] == 'TSR-SPRF')]

# COMMAND ----------

# Define a function to fit ECOD model and return the fitted model and outlier scores
def fit_ecod_model(df):
    
    # Fit
    y = df['value'].values.reshape(-1,1)
    clf = ECOD()
    clf.fit(y)

    # Get the prediction labels
    outliers = clf.labels_ 
    # Outlier scores
    outlier_scores = clf.decision_scores_

    # Add columns to dataframe
    df['is_anomaly'] = outliers

    return df

# COMMAND ----------

# Fit ECOD models for row counts
general_rows = fit_ecod_model(general_rows)
tcn_rows = fit_ecod_model(tcn_rows)
lft_rows = fit_ecod_model(lft_rows)
prorating_rows = fit_ecod_model(prorating_rows)
ret_rows = fit_ecod_model(ret_rows)
eml_rows = fit_ecod_model(eml_rows)
hot_rows = fit_ecod_model(hot_rows)
sprf_rows = fit_ecod_model(sprf_rows)

# Fit ECOD models for duplicates
general_dupl = fit_ecod_model(general_dupl)
tcn_dupl = fit_ecod_model(tcn_dupl)
lft_dupl = fit_ecod_model(lft_dupl)
prorating_dupl = fit_ecod_model(prorating_dupl)
ret_dupl = fit_ecod_model(ret_dupl)
eml_dupl = fit_ecod_model(eml_dupl)
hot_dupl = fit_ecod_model(hot_dupl)
sprf_dupl = fit_ecod_model(sprf_dupl)

# COMMAND ----------

result_anomalies = pd.concat([general_rows, tcn_rows, lft_rows, prorating_rows, ret_rows,eml_rows, hot_rows, sprf_rows, general_dupl, tcn_dupl, lft_dupl, prorating_dupl, ret_dupl, eml_dupl, hot_dupl, sprf_dupl], ignore_index=True)

# COMMAND ----------

result_anomalies.head()

# COMMAND ----------

time_compl = results.loc[(results['kpi'] == 'completeness') | (results['kpi'] == 'timeliness')]
time_compl['is_anomaly'] = None

# COMMAND ----------

results_with_anomalies = pd.concat([result_anomalies, time_compl], axis=0)

# COMMAND ----------

results_with_anomalies

# COMMAND ----------

results_dashboard = spark.createDataFrame(results_with_anomalies)

# COMMAND ----------

display(results_dashboard)

# COMMAND ----------

results_dashboard.count()

# COMMAND ----------


