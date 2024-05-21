## ti-tables Data Quality Solution ✨

This repository keeps your ti-tables output datasets clean! Let's explore what's inside:

**Maintaining Data Quality:**

* **`src` directory:** Source code, this folder holds the code that stores the data quality checks to be run on ti-tables.
  
    * **`data_quality_checks.py`:** This script is the heart of the operation. It contains all the checks needed to ensure the health of your data in the `ti-tables-ticketing`, `ti-tables-reservation`, and `ti-reports-coupons` tables. <br>
      Executes the following checks:
        * **Row count** with dataset especific key groupings.
        * **Duplicate count** with dataset especific key groupings.
        * **Completeness ratio** by date and colulmn.
        * **Date completeness** to get timeliness of datasets (data to be generated when expected).
          
    * **`checks_run.ipynb`:** This notebook puts the data quality checks to the test!  It runs the checks on a sample ticketing dataframe, giving you a glimpse into how your actual data will be analyzed.


**Testing Thoroughly:**

* **`tests` directory:** Behind the scenes, this folder holds the code that ensures the data quality checks themselves are functioning properly.
  
    * **`test_dq_checks.py`:**  Here's where the test definitions for the data quality checks reside. Asserts succesful loading of data, run of all checks and generation of KPI results output table.
      
    * **`test_run.ipynb`:** Execute this notebook to see the test results in action!

This repository provides a clear and well-organized approach to keeping your ticketing data in top shape.
