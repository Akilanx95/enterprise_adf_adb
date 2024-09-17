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


# Enables optimizations that reduce the amount of data read during queries
# spark.conf.set("spark.databricks.delta.optimizeRead.enabled", "true") # Enabled by default

# Enables Adaptive Query Execution (AQE)
# spark.conf.set("spark.sql.adaptive.enabled", "true") # Enabled by default

# Enables partition coalescing in AQE to reduce the number of partitions read
# spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Enables skewed join optimization in AQE
# spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Enables logging of Spark events for monitoring and troubleshooting
# spark.conf.set("spark.eventLog.enabled", "true") #Already enabled by default

# Controls whether retention duration checks are enforced when running vacuum on Delta tables
# spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

# Enables compression for in-memory columnar storage
# spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")

# Sets the default isolation level for Delta transactions
# spark.conf.set("spark.databricks.delta.properties.defaults.isolationLevel", "Serializable") 

spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
