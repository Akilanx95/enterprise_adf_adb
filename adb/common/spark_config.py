# Databricks notebook source
# DBTITLE 1,Import Secrets
client_id = dbutils.secrets.get("azurekv-scope","sp-client-id-adb")
client_secret = dbutils.secrets.get("azurekv-scope","sp-client-secret-adb")
tenant_id = dbutils.secrets.get("azurekv-scope","sp-tenant-id-adb")

# COMMAND ----------

# DBTITLE 1,Config
spark.conf.set("spark.databricks.delta.optimizeWrite", "true")

# Enables automatic compaction of small files into larger ones during writes to Delta tables
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true") # Needed

# Controls the number of partitions used when shuffling data for joins or aggregations
spark.conf.set("spark.sql.shuffle.partitions", "auto")

# Sets the batch size for in-memory columnar storage
spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "10000")

#spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1 * 1024 * 1024 * 1024)  
