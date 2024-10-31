# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC | **Authors**                             | Source_Schema | Target_Schema | Last_Updated_On | Last_Updated_By |
# MAGIC |-----------------------------------------|---------------|---------------|-----------------|-----------------|
# MAGIC | *akilan_venkatesantgr@cotyinc.com*      | Bronze        | Silver        | 2024-10-29      | Akilan V        |
# MAGIC | *akshay_kudale-tgr@cotyinc.com*         |               |               |                 |                 |
# MAGIC
# MAGIC # Overview
# MAGIC
# MAGIC This notebook is part of the Tiger Framework. The process involves moving data from the bronze layer to the silver layer. The Tiger Framework currently handles sixty-three tables, with thirty-one being incremental tables and thirty-two being full load tables. The initial step involves importing necessary functions from the `utils` notebook.
# MAGIC
# MAGIC ### ETL Process Overview
# MAGIC
# MAGIC | **Steps** | **Action** | **Substeps / Description** |
# MAGIC |---------------|-----------------|--------------|
# MAGIC | **Step 1** | **Import Functions** | The necessary functions for data processing are imported from the `utils` notebook. |
# MAGIC | **Step 2** | **Enable Spark Configurations** | The Spark properties like `OptimizedWrites` and `AutoCompact` are enabled for improved performance. |
# MAGIC | **Step 3** | **Receive Inputs** | Various inputs like `ModuleID`, `ModuleAuditID`, `ReloadFlag`, and `ReloadTables` are set up: <br> - **ModuleID:** Indicates the ETL segment (e.g., '3' for bronze to silver). <br> - **ModuleAuditID:** Generated for each ETL run to track the process. <br> - **ReloadFlag & ReloadTables:** Allow on-demand processing of specific tables. |
# MAGIC | **Step 4** | **Retrieve Metadata** | The metadata is fetched from the SQL database using the provided `ModuleID`. <br> - The metadata is then passed to the `ThreadPoolExecutor`, which processes up to sixty tables concurrently. |
# MAGIC | **Step 5** | **Initiate ETL for Silver Layer** | Detailed steps include: <br> - **Module Audit ID Creation:** Indicates ETL initiation. <br> - **Retrieve Metadata:** Fetch metadata from SQL based on `ModuleID`. <br> - **Task Audit ID Creation:** For each table, a task audit ID is created, indicating that the process for that table has started. <br> - **Filter Versions:** Using Change Data Feed (CDF) in Databricks, the required versions are filtered. <br> - **Apply Schema Transformations:** Schema transformations and enforcements are applied to the data. <br> - **Data Merge/Insert:** - For incremental loads, data is merged. <br> - For full loads, insert overwrite operations are performed. <br> - **Update Watermarks:** Update the watermark for each table after processing. <br> - **Task Audit ID Update:** Reflect the completion status for each task. <br> - **Module Audit ID Update:** Marks the end of the ETL process. |
# MAGIC
# MAGIC ### Silver Ingestion Function
# MAGIC
# MAGIC The function `runParallelSilver` manages the silver layer ingestion, handling tasks like metadata retrieval, version filtering, data transformation, merging/insertion, and watermark updates.
# MAGIC

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true") # enabled
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true") # enabled

# COMMAND ----------

dbutils.widgets.text("module_ID", "3", "Module ID")
moduleid = dbutils.widgets.get("module_ID")

dbutils.widgets.text("module_audit_id", "1", "Module Audit ID")
module_audit_id = dbutils.widgets.get("module_audit_id")

dbutils.widgets.text("reload_flag", "no", "Reload_flag")
reload_flag= dbutils.widgets.get("reload_flag")

dbutils.widgets.text("reload_tables", "no", "Reload_tables")
reload_tables = dbutils.widgets.get("reload_tables")

# COMMAND ----------

# DBTITLE 1,silver data loading
df_metadata = get_metadata(moduleid)
df_metadata = df_metadata.orderBy(asc("load_type"))

if reload_flag == 'yes':
    tables_list = [table.strip() for table in reload_tables.replace("'", "").split(",")]
    df_metadata = df_metadata.filter(df_metadata.source_object.isin(tables_list))

rows = df_metadata.select("project_name", "source_object", "taskid", "appid", "domain", 
                          "custom_expression", "watermark_value", "load_type", "target_property", 
                          "table_transformation", "data_layer", "source_property","batch_id").collect() 

distinct_batch_ids = sorted(list(set([row.batch_id for row in rows])))

all_futures = []

for batch_id in distinct_batch_ids:
    batch_rows = [row for row in rows if row.batch_id == batch_id]
    with ThreadPoolExecutor(max_workers=60) as executor:
        futures = [executor.submit(run_parallel_silver, metadata_loader(row)) for row in batch_rows]
        all_futures.extend(futures)

# COMMAND ----------

spark.createDataFrame([status.result() for status in all_futures], schema="table string, start_version string, end_version string, status string").display()
