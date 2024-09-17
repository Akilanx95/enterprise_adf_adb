# Databricks notebook source
# MAGIC %run ../common/utils

# COMMAND ----------

dbutils.widgets.text("module_ID", "3", "Module ID")
moduleid = dbutils.widgets.get("module_ID")

dbutils.widgets.text("module_audit_id", "1", "Module Audit ID")
module_audit_id = dbutils.widgets.get("module_audit_id")

# COMMAND ----------

# DBTITLE 1,silver data loading
df_metadata = get_metadata(moduleid)
rows = df_metadata.select("project_name", "source_object", "taskid", "appid", "domain", 
                          "custom_expression", "watermark_value", "load_type", "target_property", 
                          "table_transformation", "data_layer", "source_property").collect() 
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(run_parallel_silver, metadata_loader(row)) for row in rows]

# COMMAND ----------

spark.createDataFrame([status.result() for status in futures], schema="table string, start_version string, end_version string, status string").drop("start_version", "end_version").display()
