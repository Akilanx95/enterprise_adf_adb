# Databricks notebook source
dbutils.widgets.text("Catalog_name","")
dbutils.widgets.text("db_name_gold","")
dbutils.widgets.text("db_name_silver","")

# COMMAND ----------

Catalog_name = dbutils.widgets.get("Catalog_name")
db_name_gold = dbutils.widgets.get("db_name_gold")
db_name_silver = dbutils.widgets.get("db_name_silver")

# COMMAND ----------

print(Catalog_name)
print(db_name_gold)
print(db_name_silver)

# COMMAND ----------

# DBTITLE 1,temp view
spark.sql(f"""DROP VIEW IF EXISTS remessa_historica_src""")
spark.sql(f"""CREATE temporary view remessa_historica_src AS
SELECT
        erdat AS dt_remessa,
        erzet AS hr_remessa,
        to_timestamp(concat(erdat, ' ', left(erzet, 8)), 'yyyy-MM-dd HHmmss') AS dthr_remessa,
        vbelv AS cd_ordem_venda,
        posnv AS cd_item_ordem_venda,
        vbeln AS cd_remessa,
        meins AS cd_material_unidade_venda,
        sum(rfmng) AS qt_remessa 
    FROM
         {Catalog_name}.{db_name_silver}.vbfa
    WHERE
        RFMNG <> 0
        AND VBTYP_N = 'J'
        AND vbfa.ingested_timestamp >= (SELECT coalesce(max(updated_timestamp),'1900-01-01') from {Catalog_name}.{db_name_gold}.remessa_historica)
    GROUP BY
        erdat,
        erzet,
        vbelv,
        posnv,
        vbeln,
        meins""")

# COMMAND ----------

max_dt_remessa = spark.sql(f"SELECT max(dt_remessa) FROM remessa_historica_src").collect()[0][0]

# COMMAND ----------

spark.sql(f"""
WITH Ranked_Remessa AS (SELECT *,RANK() OVER (PARTITION BY cd_ordem_venda ORDER BY dt_remessa ASC) AS rank_order FROM remessa_historica_src),
Filtered_Remessa AS (SELECT remessa.*, existing.dt_remessa AS existing_target_remessa 
  FROM Ranked_Remessa remessa 
  LEFT JOIN (select  cd_ordem_venda,dt_remessa from {Catalog_name}.{db_name_gold}.remessa_historica 
  qualify row_number() over (partition by cd_ordem_venda order by dt_remessa asc )=1 ) existing
  ON remessa.cd_ordem_venda = existing.cd_ordem_venda
    WHERE
        remessa.rank_order = 1 AND (existing.dt_remessa = remessa.dt_remessa OR existing.dt_remessa IS NULL)
)
MERGE INTO {Catalog_name}.{db_name_gold}.remessa_historica AS target
USING Filtered_Remessa AS source
    ON  
        target.dt_remessa=source.dt_remessa
    AND 
        target.hr_remessa = source.hr_remessa
    AND   
         target.dthr_remessa = source.dthr_remessa 
    AND 
        target.cd_ordem_venda = source.cd_ordem_venda 
    AND 
        target.cd_item_ordem_venda = source.cd_item_ordem_venda 
    AND 
        target.cd_remessa = source.cd_remessa
    AND 
        target.cd_material_unidade_venda = source.cd_material_unidade_venda

WHEN MATCHED THEN
UPDATE SET
    target.qt_remessa = source.qt_remessa,
    target.updated_by = current_user(),
    target.updated_timestamp = current_timestamp()

WHEN NOT MATCHED BY TARGET THEN
INSERT (
    dt_remessa, hr_remessa, dthr_remessa, cd_ordem_venda, cd_item_ordem_venda, cd_remessa, cd_material_unidade_venda, qt_remessa, created_by, created_timestamp, updated_by, updated_timestamp)
VALUES (
    source.dt_remessa, source.hr_remessa, source.dthr_remessa, source.cd_ordem_venda, source.cd_item_ordem_venda, source.cd_remessa, source.cd_material_unidade_venda, source.qt_remessa, current_user(),current_timestamp(),current_user(),current_timestamp())
WHEN NOT MATCHED BY SOURCE AND target.dt_remessa between date_sub('{max_dt_remessa}', 3) and '{max_dt_remessa}' THEN
    DELETE;""")


# COMMAND ----------

# %sql
# OPTIMIZE cotybrcatalog_prd.gold_customer_service.remessa_historica
