# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# COMMAND ----------

# DBTITLE 1,notebook_parameters
dbutils.widgets.text("gold_include_tables", "no", "gold_include_tables")
gold_include_tables = dbutils.widgets.get("gold_include_tables")

# dbutils.widgets.text("gold_exclude_tables", "no", "gold_exclude_tables")
# gold_exclude_tables = dbutils.widgets.get("gold_exclude_tables")

dbutils.widgets.text("platinum_include_tables", "no", "platinum_include_tables")
platinum_include_tables = dbutils.widgets.get("platinum_include_tables")

# dbutils.widgets.text("platinum_exclude_tables", "no", "platinum_exclude_tables")
# platinum_exclude_tables = dbutils.widgets.get("platinum_exclude_tables")

# COMMAND ----------

# DBTITLE 1,Gold_parameters
Catalog_name = 'cotybrcatalog_prd'
db_name_gold = 'gold_customer_service'
db_name_silver = 'silver_sap_hana'

# COMMAND ----------

# DBTITLE 1,Platinum_parameters
catalog_name = 'cotybrcatalog_prd'
db_name_platinum = 'platinum_customer_service'

# COMMAND ----------

# DBTITLE 1,Gold_Notebooks
errorCount = 0
lastExceptionMessage = "" 
l_tables = ['cotybrcatalog_prd_gold_customer_service_canal',
            'cotybrcatalog_prd_gold_customer_service_empresa',
            'cotybrcatalog_prd_gold_customer_service_centro_distribuicao',
            'cotybrcatalog_prd_gold_customer_service_material',
            'cotybrcatalog_prd_gold_customer_service_area_venda_ordem_venda',
            'cotybrcatalog_prd_gold_customer_service_cliente',
            'cotybrcatalog_prd_gold_customer_service_faturamento_notas_fiscais',
            'cotybrcatalog_prd_gold_customer_service_ordem_venda',
            'cotybrcatalog_prd_gold_customer_service_ordem_venda_data_recusa',
            'cotybrcatalog_prd_gold_customer_service_pedido_recusado_edi',
            'cotybrcatalog_prd_gold_customer_service_remessa_historica',
            'cotybrcatalog_prd_gold_customer_service_taxa_cambio',
            'cotybrcatalog_prd_gold_customer_service_motivo_recusa_class_status_scd']
            
if gold_include_tables!='no':
    l_tables = [x for x in l_tables if x.strip().lower() in [table.strip().lower() for table in gold_include_tables.split(',')]]
# elif gold_exclude_tables!='no' and gold_include_tables=='no':
#     l_tables = [x for x in l_tables if x.strip().lower() not in [table.strip().lower() for table in gold_exclude_tables.split(',')]]
# elif gold_include_tables!='no' and gold_exclude_tables!='no':
#     dbutils.notebook.exit("Please provide either include_tables or exclude_tables for gold")

def run_notebook(table_name):
    try:
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", f"{table_name}")
        print("running the notebook for " +table_name)
        dbutils.notebook.run(f"/Workspace/Repos/COTY-CONTROL-TOWER/coty-data-lake/adb/coty_development_notebooks/gold/{table_name}",0,{"Catalog_name": Catalog_name,"db_name_gold":db_name_gold,"db_name_silver":db_name_silver})
        print("completed the notebook for " +table_name)
    except Exception as ex:
        errorCount += 1
        lastExceptionMessage = str(ex)
        print("Notebook execution failed for "+table_name)
        print(ex)

with ThreadPoolExecutor(max_workers=15) as executor:
     futures = [executor.submit(run_notebook, table_name) for table_name in l_tables]


if errorCount > 0:
    raise Exception(f"Notebook execution failed with {lastExceptionMessage}")


# COMMAND ----------

# DBTITLE 1,Platinum_Notebooks
errorCount = 0
lastExceptionMessage = "" 
l_tables = [
            'platinum_customer_service_dim_area_venda',
            'platinum_customer_service_dim_canal',
            'platinum_customer_service_dim_centro_distribuicao',
            'platinum_customer_service_dim_cliente',
            'platinum_customer_service_dim_empresa',
            'platinum_customer_service_dim_material',
            'platinum_customer_service_fato_ordem_venda',
            'platinum_customer_service_fato_recusa',
            'platinum_customer_service_fato_faturamento_notas_fiscais',
            'platinum_customer_sevice_fato_pedido_recusado_edi',
            'platinum_customer_service_fato_remesssas_primeiro_dia'
            ]

if platinum_include_tables!='no':
    l_tables = [x for x in l_tables if x.strip().lower() in [table.strip().lower() for table in platinum_include_tables.split(',')]]

# elif platinum_exclude_tables!='no' and platinum_include_tables=='no':
#     l_tables = [x for x in l_tables if x.strip().lower() not in [table.strip().lower() for table in platinum_exclude_tables.split(',')]]
    
# elif platinum_include_tables!='no' and platinum_include_tables!='no':
#     dbutils.notebook.exit("Please provide either include_tables or exclude_tables for platinum")

def run_notebook(table_name):
    try:
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", f"{table_name}")
        print("running the notebook for "+table_name)
        dbutils.notebook.run(f"/Workspace/Repos/COTY-CONTROL-TOWER/coty-data-lake/adb/coty_development_notebooks/platinum/{table_name}",0,{"catalog_name": catalog_name,"db_name_platinum":db_name_platinum,"db_name_gold":db_name_gold})
        print("completed the notebook for "+table_name)
    except Exception as ex:
        errorCount += 1
        lastExceptionMessage = str(ex)
        print("Notebook execution failed for "+table_name)
        print(ex)

if platinum_include_tables=='no' or 'platinum_customer_service_dim_motivo_recusa_scd' in [table.strip().lower() for table in platinum_include_tables.split(',')]:
    print("running the notebook for dim_motivo_recusa_scd")   
    dbutils.notebook.run(f"/Workspace/Repos/COTY-CONTROL-TOWER/coty-data-lake/adb/coty_development_notebooks/platinum/platinum_customer_service_dim_motivo_recusa_scd",0,{"catalog_name": catalog_name,"db_name_platinum":db_name_platinum,"db_name_gold":db_name_gold})
    print("completed the notebook for dim_motivo_recusa_scd") 

with ThreadPoolExecutor(max_workers=11) as executor:
     futures = [executor.submit(run_notebook, table_name) for table_name in l_tables]

if errorCount > 0:
    raise Exception(f"Notebook execution failed with {lastExceptionMessage}")

