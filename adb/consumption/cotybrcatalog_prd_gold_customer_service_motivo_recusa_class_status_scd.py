# Databricks notebook source
dbutils.widgets.text("Catalog_name","")
dbutils.widgets.text("db_name_gold","")
dbutils.widgets.text("db_name_silver","")

# COMMAND ----------

Catalog_name = dbutils.widgets.get("Catalog_name")
db_name_gold = dbutils.widgets.get("db_name_gold")
db_name_silver = dbutils.widgets.get("db_name_silver")

# COMMAND ----------

spark.sql(f"""DROP VIEW IF EXISTS motivo_recusa_class_status_scd_src""")
spark.sql(f"""CREATE temporary view motivo_recusa_class_status_scd_src AS
          with union_data as (SELECT
            tvagt.abgru as cd_motivo_recusa,
            tvagt.bezei as nm_motivo_recusa
           -- ,"tvagt" as tbl_name
           FROM 
              silver_sap_hana.tvagt tvagt WHERE SPRAS='P'
        UNION 
            SELECT 
              zsdt.zedi_cod_mrecusa as cd_motivo_recusa,
              zsdt.zedi_motivo_recusa as nm_motivo_recusa
             -- ,"zsdtedi011" as tbl_name
              FROM {db_name_silver}.zsdtedi031 zsdt )
        SELECT  
          src.*,tgt.sk_motivo_recusa 
        FROM 
          (SELECT 
            ud.*,coalesce(motivo.status,'DE PARA NAO ENCONTRADO') as status,coalesce(motivo.nm_classificacao_motivo_recusa,'DE PARA NAO ENCONTRADO') as nm_classificacao_motivo_recusa 
          FROM 
          union_data ud 
          LEFT JOIN silver_sap_hana.motivo_recusa_class_status motivo on ud.nm_motivo_recusa= motivo.nm_motivo_recuisa ) src
          LEFT JOIN (select * from cotybrcatalog_prd.gold_customer_service.motivo_recusa_class_status_scd where actual=true) tgt 
          ON src.cd_motivo_recusa=tgt.cd_motivo_recusa and src.nm_motivo_recusa=tgt.nm_motivo_recusa and src.status=tgt.status
          AND src.nm_classificacao_motivo_recusa=tgt.nm_classificacao_motivo_recusa
          WHERE tgt.sk_motivo_recusa IS NULL
 """)

# COMMAND ----------

spark.sql(f"""
MERGE INTO {Catalog_name}.{db_name_gold}.motivo_recusa_class_status_scd target USING (
 SELECT
    (select coalesce(max(sk_motivo_recusa), 0) from cotybrcatalog_prd.gold_customer_service.motivo_recusa_class_status_scd ) + row_number() over (partition by 1 order by 1) as sk_motivo_recusa,
    cd_motivo_recusa,
    nm_motivo_recusa,
    status,
    nm_classificacao_motivo_recusa,
    current_date() as start_date,
    null as end_date,
    true as actual,
    current_user() as created_by,
    current_timestamp() as created_timestamp
  FROM
    motivo_recusa_class_status_scd_src
  UNION ALL
  SELECT
    sk_motivo_recusa,
    cd_motivo_recusa,
    nm_motivo_recusa,
    status,
    nm_classificacao_motivo_recusa,
    start_date,
    end_date,
    actual,
    created_timestamp,
    created_by
  FROM
    cotybrcatalog_prd.gold_customer_service.motivo_recusa_class_status_scd
  WHERE
    cd_motivo_recusa IN (
      SELECT
        cd_motivo_recusa
      from
        motivo_recusa_class_status_scd_src
    )
) source on target.sk_motivo_recusa = source.sk_motivo_recusa
WHEN MATCHED THEN
UPDATE
SET
  target.end_date = current_date(),
  target.updated_by = current_user(),
  target.updated_timestamp = current_timestamp(), 
  target.actual = false
  WHEN NOT MATCHED THEN
INSERT
  (
    sk_motivo_recusa,
    cd_motivo_recusa,
    nm_motivo_recusa,
    status,
    nm_classificacao_motivo_recusa,
    start_date,
    end_date,
    actual,
    created_by,
    created_timestamp,
    updated_by,
    updated_timestamp
  )
VALUES
  (
    source.sk_motivo_recusa,
    source.cd_motivo_recusa,
    source.nm_motivo_recusa,
    source.status,
    source.nm_classificacao_motivo_recusa,
    source.start_date,
    source.end_date,
    source.actual,
    current_user(),
    current_timestamp(),
    current_user(),
    current_timestamp()
  )""")

# COMMAND ----------

# %sql
# OPTIMIZE cotybrcatalog_prd.gold_customer_service.motivo_recusa_class_status_scd
