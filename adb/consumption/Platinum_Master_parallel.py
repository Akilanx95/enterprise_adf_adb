# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------

catalog_name = 'cotybrcatalog_prd'
db_name_gold='gold_customer_service'
db_name_platinum = 'platinum_customer_service'

# COMMAND ----------

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

print("running the notebook for dim_motivo_recusa_scd")   
dbutils.notebook.run(f"/Workspace/Repos/COTY-CONTROL-TOWER/coty-data-lake/adb/coty_development_notebooks/platinum/platinum_customer_service_dim_motivo_recusa_scd",0,{"catalog_name": catalog_name,"db_name_platinum":db_name_platinum,"db_name_gold":db_name_gold})
print("completed the notebook for dim_motivo_recusa_scd") 

with ThreadPoolExecutor(max_workers=11) as executor:
     futures = [executor.submit(run_notebook, table_name) for table_name in l_tables]

if errorCount > 0:
    raise Exception(f"Notebook execution failed with {lastExceptionMessage}")

