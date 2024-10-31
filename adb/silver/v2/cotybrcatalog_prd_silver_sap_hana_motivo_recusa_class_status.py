# Databricks notebook source
# DBTITLE 1,Install openpyxl
# MAGIC %pip install openpyxl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Add Widgets
dbutils.widgets.text("Catalog_name","")
dbutils.widgets.text("db_name_silver","")
dbutils.widgets.text("table_name","")
dbutils.widgets.text("file_path", "")

# COMMAND ----------

# DBTITLE 1,variable declaration
required_columns = ['Recusa EDI (DE)', 'Status', 'PARA']

Catalog_name = dbutils.widgets.get("Catalog_name")
db_name_silver = dbutils.widgets.get("db_name_silver")
table_name = dbutils.widgets.get("table_name")
file_path=dbutils.widgets.get("file_path")

# COMMAND ----------

# DBTITLE 1,Install Libraries
import pandas as pd
from datetime import date

# COMMAND ----------

# DBTITLE 1,Variable Declaration
bronze_path = f'{file_path}'
tgt_file_name=file_path.split("/")[-1]

local_path=f'file:///Workspace/Shared/excel_file_upload/{table_name}/{tgt_file_name}'
target_table = f'{Catalog_name}.{db_name_silver}.{table_name}'

print(bronze_path)
print(local_path)
print(target_table)

# COMMAND ----------

# DBTITLE 1,transformations
# Define custom transformations using selectExpr
custom_expression = [
    "cast(`Recusa EDI (DE)` as string) as nm_motivo_recuisa",
    "cast(Status as string) as status",
    "cast(PARA as string) as nm_classificacao_motivo_recusa",
    "current_user() as ingested_by",
    "current_timestamp() as ingested_timestamp"
]

# COMMAND ----------

# DBTITLE 1,Write to target
try:
    if not spark.catalog.tableExists(f"{Catalog_name}.{db_name_silver}.{table_name}"):
        print("Target silver table does not exist. Please create the target table before proceeding.")

    if not dbutils.fs.ls(bronze_path):
        raise FileNotFoundError(f"File not found. Make sure the file exists in {bronze_path}.")

    dbutils.fs.cp(bronze_path, local_path)
    df = pd.read_excel(local_path)
    spark_df = spark.createDataFrame(df)

    missing_columns = [col for col in required_columns if col not in spark_df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

    spark_df = spark_df.selectExpr(*custom_expression)
    spark_df.write.mode("overwrite").saveAsTable(target_table)

except Exception as e:
    print(f"An error occurred: {e}")
