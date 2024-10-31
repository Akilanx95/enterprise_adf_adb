# Databricks notebook source
# MAGIC %md
# MAGIC | **Authors**                             | Source_Layer | Target_Layer | Last_Updated_On | Last_Updated_By |
# MAGIC |-----------------------------------------|---------------|---------------|-----------------|-----------------|
# MAGIC | *akilan_venkatesantgr@cotyinc.com*      | Landing (ADLS) | Bronze        | 2024-10-29      | Akilan V        |
# MAGIC | *nikhil_ranga-tgr@cotyinc.com*          |               |               |                 |                  |
# MAGIC
# MAGIC # Overview
# MAGIC
# MAGIC This notebook is a part of the ETL process within the Tiger Data Framework. The purpose of this process is to move data from the *Landing* layer (ADLS) to the *Bronze* layer. Unlike the ETL process in the silver layer, this process does not involve a merge strategy; instead, it performs a simple append operation with the inclusion of a timestamp and other audit fields.
# MAGIC
# MAGIC ### ETL Process Overview
# MAGIC
# MAGIC | **Main Step** | **Description** | **Substeps** |
# MAGIC |---------------|-----------------|--------------|
# MAGIC | **Step 1** | **Import Functions** | The necessary functions for data processing are imported from the `utils` notebook. |
# MAGIC | **Step 2** | **Receive Inputs** | Various inputs like `ModuleID`, `ModuleAuditID`, `ReloadFlag`, and `ReloadTables` are set up: <br> - **ModuleID:** Indicates the ETL segment (e.g., '2' for landing to bronze). <br> - **ModuleAuditID:** Generated for each ETL run to track the process. <br> - **ReloadFlag & ReloadTables:** Allow on-demand processing of specific tables. |
# MAGIC | **Step 3** | **Retrieve Metadata** | The metadata is fetched from the SQL database using the provided `ModuleID`. <br> - The metadata is passed to the `ThreadPoolExecutor`, which processes up to sixty tables concurrently. |
# MAGIC | **Step 4** | **Initiate ETL for Bronze Layer** | Detailed steps include: <br> - **Module Audit ID Creation:** Indicates ETL initiation. <br> - **Retrieve Metadata:** Fetch metadata from SQL based on `ModuleID`. <br> - **Task Audit ID Creation:** For each table, a task audit ID is created, indicating that the process for that table has started. <br> - **Data Appending:** The data is appended with timestamp and audit fields. <br> - **Update Watermarks:** Watermarks are updated for each table after processing. <br> - **Task Audit ID Update:** Reflects the completion status for each task. <br> - **Module Audit ID Update:** Marks the end of the ETL process. |
# MAGIC
# MAGIC ### Bronze Ingestion Function
# MAGIC
# MAGIC The function `run_parallel_bronze` is responsible for managing the ingestion process from the *Landing* to the *Bronze* layer, handling tasks like metadata retrieval, data appending, and audit logging.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Run Config
# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,parameters
dbutils.widgets.text("module_ID", "2", "Module ID")
moduleid = dbutils.widgets.get("module_ID")

dbutils.widgets.text("module_audit_id", "1", "Module Audit ID")
module_audit_id = dbutils.widgets.get("module_audit_id")

dbutils.widgets.text("reload_flag", "no", "Reload_flag")
reload_flag= dbutils.widgets.get("reload_flag")

dbutils.widgets.text("reload_tables", "no", "Reload_tables")
reload_tables = dbutils.widgets.get("reload_tables")

# COMMAND ----------

# DBTITLE 1,bronze data loading
df_metadata = get_metadata(moduleid)
df_metadata = df_metadata.orderBy(asc("load_type"))

if reload_flag == 'yes':
    tables_list = [table.strip() for table in reload_tables.replace("'", "").split(",")]
    df_metadata = df_metadata.filter(df_metadata.source_object.isin(tables_list))

rows = df_metadata.select("project_name", "data_layer", "source_object", "taskid", "appid", "domain","watermark_value", "source_property","custom_expression","target_property","table_transformation","batch_id").collect()

distinct_batch_ids = sorted(list(set([row.batch_id for row in rows])))

all_futures = []
for batch_id in distinct_batch_ids:
    batch_rows = [row for row in rows if row.batch_id == batch_id]
    with ThreadPoolExecutor(max_workers=60) as executor:
        futures = [executor.submit(run_parallel_bronze, metadata_loader(row)) for row in batch_rows]
        all_futures.extend(futures)

# COMMAND ----------

spark.createDataFrame([status.result() for status in all_futures],schema="table string,status string").display()
