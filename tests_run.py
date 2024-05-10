# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

import pytest
import sys

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# Run pytest.
retcode = pytest.main([".", "-v", "-p", "no:cacheprovider"])

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."

# COMMAND ----------

df = spark.sql("SELECT * FROM ac_stg_green.raw_document")

# COMMAND ----------

from dq_function import tableExists

# COMMAND ----------

spark.catalog.tableExists("raw_document", "ac_stg_green")

# COMMAND ----------

# MAGIC %md
# MAGIC Problem in test function: have called spark.catalog.tableExists twice since tableExists is already defined as function that returns spark.catalog.tableExists in function_call

# COMMAND ----------


