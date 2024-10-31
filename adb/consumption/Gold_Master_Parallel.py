# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------

Catalog_name = 'cotybrcatalog_prd'
db_name_gold = 'gold_customer_service'
db_name_silver = 'silver_sap_hana'

# COMMAND ----------

print(Catalog_name)
print(db_name_gold)
print(db_name_silver)

# COMMAND ----------

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

# cotybrcatalog_prd_gold_customer_service_canal,
# cotybrcatalog_prd_gold_customer_service_empresa,
# cotybrcatalog_prd_gold_customer_service_centro_distribuicao,
# cotybrcatalog_prd_gold_customer_service_material,
# cotybrcatalog_prd_gold_customer_service_area_venda_ordem_venda,
# cotybrcatalog_prd_gold_customer_service_cliente,
# cotybrcatalog_prd_gold_customer_service_faturamento_notas_fiscais,
# cotybrcatalog_prd_gold_customer_service_ordem_venda,
# cotybrcatalog_prd_gold_customer_service_ordem_venda_data_recusa,
# cotybrcatalog_prd_gold_customer_service_pedido_recusado_edi,
# cotybrcatalog_prd_gold_customer_service_remessa_historica,
# cotybrcatalog_prd_gold_customer_service_taxa_cambio,
# cotybrcatalog_prd_gold_customer_service_motivo_recusa_class_status_scd
