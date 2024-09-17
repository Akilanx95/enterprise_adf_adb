# Databricks notebook source
# DBTITLE 1,Run Config
# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,parameters
dbutils.widgets.text("module_ID", "2", "Module ID")
moduleid = dbutils.widgets.get("module_ID")

dbutils.widgets.text("module_audit_id", "1", "Module Audit ID")
module_audit_id = dbutils.widgets.get("module_audit_id")

# COMMAND ----------

# DBTITLE 1,bronze data loading
df_metadata = get_metadata(moduleid)
rows = df_metadata.select("project_name", "data_layer", "source_object", "taskid", "appid", "domain","watermark_value", "source_property","custom_expression","target_property","table_transformation").collect()
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(run_parallel_bronze, metadata_loader(row)) for row in rows]
    

# COMMAND ----------

spark.createDataFrame([status.result() for status in futures],schema="table string,status string").display()
