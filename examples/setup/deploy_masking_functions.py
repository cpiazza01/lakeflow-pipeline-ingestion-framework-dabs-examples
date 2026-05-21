# Databricks notebook source

# COMMAND ----------

# Databricks injects 'spark' and 'dbutils' at runtime; declare them here to
# satisfy the local linter.
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pyspark.sql import SparkSession  # type: ignore
    from databricks.sdk.runtime import DBUtils
    spark: SparkSession
    dbutils: DBUtils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Masking Functions
# MAGIC Deploys Unity Catalog column masking functions used by the lakeflow-pipeline-ingestion-framework-examples pipeline.
# MAGIC Assumes the target catalog and `functions` schema already exist.
# MAGIC
# MAGIC **Required parameter:** `catalog` — the Unity Catalog name for the target environment.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
catalog = dbutils.widgets.get("catalog")

assert catalog, "The 'catalog' parameter is required."
print(f"Deploying masking functions to: {catalog}.functions")

# COMMAND ----------

# Masks SSN — shows full value to members of 'pii-authorized',
# otherwise returns ***-**-XXXX (last 4 digits visible).
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {catalog}.functions.mask_ssn(ssn STRING)
    RETURNS STRING
    RETURN CASE
      WHEN is_account_group_member('pii-authorized') THEN ssn
      ELSE CONCAT('***-**-', RIGHT(ssn, 4))
    END
""")
print(f"OK: {catalog}.functions.mask_ssn")

# COMMAND ----------

# Masks any PII string column — shows full value to members of
# 'pii-authorized', otherwise returns a fixed mask token.
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {catalog}.functions.mask_pii_string(value STRING)
    RETURNS STRING
    RETURN CASE
      WHEN is_account_group_member('pii-authorized') THEN value
      ELSE '*** MASKED ***'
    END
""")
print(f"OK: {catalog}.functions.mask_pii_string")

# COMMAND ----------

print(f"\nDone. Masking functions deployed to {catalog}.functions")
