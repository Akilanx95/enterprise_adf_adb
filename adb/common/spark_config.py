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

# Sets the batch size for in-memory columnar storage
spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "10000")

#spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1 * 1024 * 1024 * 1024)  

# optimize reads
# Predicate pushdown allows Spark to filter data at the data source level, reducing the amount of data read into memory
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
spark.conf.set("spark.sql.parquet.mergeSchema", "false")  # Disable schema merging for faster reads

# The vectorized Parquet reader can improve performance by reading batches of rows at once
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")

# Larger Parquet block sizes can improve read throughput by reducing the number of I/O operations
spark.conf.set("parquet.block.size", 268435456)  # 256 MB


# Adjusting the number of shuffle partitions can help balance the workload across the cluster. Set this based on the number of cores in your cluster. Set this value to 2–4 times the total number of cores in your cluster. 
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Controls the number of partitions used when shuffling data for joins or aggregations
spark.conf.set("spark.sql.shuffle.partitions", "auto")

# Caching metadata can reduce the overhead of loading metadata for Parquet files
spark.conf.set("spark.sql.parquet.cacheMetadata", "true")

# Column pruning ensures that only the necessary columns are read from disk
spark.conf.set("spark.sql.optimizer.columnPruning", "true")

# Adjusting the batch size can improve memory utilization and performance
spark.conf.set("spark.sql.parquet.columnarReaderBatchSize", "4096")

# Data skipping allows Delta Lake to avoid reading files that don't match query predicates
spark.conf.set("spark.databricks.io.skipping.enabled", "true")

# Allows Spark to split large files for parallel processing
spark.conf.set("spark.sql.files.openCostInBytes", "4194304")  # 4 MB

# Choosing the right compression codec can balance between I/O and CPU usage
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

# Enable Parquet Bloom Filters (Spark 3.0+)
# Description: Bloom filters can speed up queries with selective filters.
spark.conf.set("parquet.bloom.filter.enabled", "true")
spark.conf.set("parquet.bloom.filter.expected.ndv", "1000000")  # Adjust expected number of distinct values

# Repartition the DataFrame to increase parallelism: Repartitioning data can help utilize all available cores for reading data
# df = spark.read.parquet("/path/to/parquet").repartition(200)

# Adjusting the batch size can optimize memory usage
spark.conf.set("spark.sql.parquet.columnarReaderBatchSize", "8192")

# Improves performance when dealing with a large number of files
spark.conf.set("spark.sql.sources.parallelPartitionDiscovery.threshold", "32")

# Set the desired file size to 256 MB
spark.conf.set("spark.sql.files.maxRecordsPerFile", 10000000)  # Adjust this based on your record size
spark.conf.set("spark.sql.parquet.blockSize", 256 * 1024 * 1024)  # 256 MB
spark.conf.set("spark.sql.parquet.targetRowGroupSize", 256 * 1024 * 1024)  # 256 MB

# Controls the maximum number of bytes to pack into a single partition when reading files
spark.conf.set("spark.sql.files.maxPartitionBytes", 134217728)  # 128 MB
spark.conf.set('spark.sql.files.maxPartitionBytes','512m')

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
