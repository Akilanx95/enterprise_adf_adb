# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC | **Authors**                             | **Last Updated On** | **Last Updated By**         | 
# MAGIC |-----------------------------------------|----------------------|-----------------------------|
# MAGIC | *akilan_venkatesantgr@cotyinc.com*      | 2024-10-29         | Akilan V               |
# MAGIC | *akshay_kudale-tgr@cotyinc.com*         |                      |                    |
# MAGIC | *somanath_sankarantgr@cotyinc.com*      |                      |                |
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC The **UTILS** notebook serves as a common utility notebook in the Tiger Data Framework. It contains reusable functions designed to be used across multiple notebooks, including **BronzeMaster**, **SilverMaster**, and any potential future notebooks. It acts as a centralized repository, promoting modularity and reusability across the framework.
# MAGIC
# MAGIC ### Key Concepts and Functions
# MAGIC
# MAGIC | **Category**         | **Function**        | **Description**                                                                                  |
# MAGIC |----------------------|---------------------|--------------------------------------------------------------------------------------------------|
# MAGIC | **Parent Functions** | `RunParallelSilver` | Orchestrates the silver layer ETL by invoking child functions like **InsertAudit**, **TransformData**, **WriteToSilver**, and **UpdateAuditLog**. |
# MAGIC |                      | `RunParallelBronze` | Similar to `RunParallelSilver`, it manages bronze layer ETL, calling corresponding child functions. |
# MAGIC | **Metadata Handling**| `MetadataLoader`    | Processes metadata extracted from SQL and uses named tuples to create variables for each data layer (bronze/silver). |
# MAGIC | **Database Connection** | `get_db_connection_and_token` | Establishes secure connections to SQL databases using **ServicePrincipalAuthentication** (or general DB authentication). |
# MAGIC | **Error Handling**   | `handle_exception`  | Captures exceptions during execution and updates the audit log accordingly.                      |
# MAGIC
# MAGIC ### Spark Configurations
# MAGIC
# MAGIC The UTILS notebook also includes Spark configuration settings to optimize ETL performance. Key configurations like **Broadcast Join Threshold** and **shuffle partitions** are enabled to ensure efficient data writes.
# MAGIC

# COMMAND ----------

# MAGIC %run ./spark_config

# COMMAND ----------

# DBTITLE 1,Import Libraries
# For Data processing
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, expr, lower, upper, 
    dayofmonth, month, year, row_number, when, 
    lit, substring, to_date, to_timestamp, split, 
    concat_ws, current_user,asc)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, TimestampType, DoubleType, DateType)
from functools import reduce
from delta.tables import DeltaTable
from pyspark.errors import PySparkException,AnalysisException
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# for file processing
import os
import json
from datetime import datetime,date
import json
import time
import adal

# COMMAND ----------

# DBTITLE 1,getting_db_connection
def get_db_connection_and_token(client_id, client_secret, tenant_id):
    authority = "https://login.windows.net/" + tenant_id
    resource_app_id_url = "https://database.windows.net/"
    
    context = adal.AuthenticationContext(authority)
    token = context.acquire_token_with_client_credentials(
        resource_app_id_url, 
        client_id, 
        client_secret)
    
    if token:
        access_token = token["accessToken"]
    else:
        raise Exception("Failed to acquire access token")
    
    url = "jdbc:sqlserver://usmsantdb055p.database.windows.net:1433"
    database_name = "SCM_CT_Dev_sql_01"
    azure_sql_url = f"{url};database={database_name}"
    
    properties = spark.sparkContext._gateway.jvm.java.util.Properties()
    properties.setProperty("accessToken", access_token)
    
    driver_manager = spark.sparkContext._gateway.jvm.java.sql.DriverManager
    con = driver_manager.getConnection(azure_sql_url, properties)
    
    return con, token, access_token, azure_sql_url


# COMMAND ----------

# DBTITLE 1,Function to Identify files
def get_file_list(blob_dir_path):
    """
    Retrieves list of all file paths in the directory.
    Parameters:
        blob_dir_path : str
            The path of the directory in the blob storage.
    Returns:
        list: A list of file paths found in the directory and its subdirectories.
    """
    try:
        def recursive_list_file(blob_dir_path):
            file_paths=[]
            try:
                file_list = dbutils.fs.ls(blob_dir_path)
            except Exception as e:
                print(f"Error accessing {blob_dir_path}: {str(e)}")
                return None
            for file in file_list:
                if file.isFile():
                    file_paths.append(file.path)
                else:
                    file_paths.extend(recursive_list_file(file.path))
            return file_paths
        file_list= recursive_list_file(blob_dir_path)
        return file_list
    except Exception as ex:
                print("inside get_file_list")

# COMMAND ----------

# DBTITLE 1,metadata_loader
def metadata_loader(row):
    """
    Loads metadata based on the bronze or silver layers.
    Parameters:
        row : dict
            Row of data that includes information such as data layer, project name, source object etc.
    Returns:
        Metadata : 
            A named tuple containing metadata details for bronze and silver. 
    """
    try:
        d = date.today()
        yr = d.year
        month = '{:02d}'.format(d.month)
        day = '{:02d}'.format(d.day)

        if row["data_layer"] == "raw":
            Metadata = namedtuple('Metadata', [
                'project_name', 'source_object', 'task_id', 'app_Id', 'domain', 'source_property', 'custom_expression', 
                'target_property', 'table_transformation', 'source_path', 'archive_path', 'formatted_datetime', 
                'tgt_data_catalog', 'target_table', 'data_layer'
            ])

            source_property = row["source_property"]
            source_dict = json.loads(source_property)
            src_root_path = source_dict['root_path']
            source_path = source_dict['path']

            target_property = row["target_property"]
            target_dict = json.loads(target_property)
            tgt_data_catalog = target_dict['catalog']
            target_schema = target_dict['schema']
            target_table_name = target_dict['table']
            archive_root_path = target_dict['archive_root_path']

            meta = Metadata(
                project_name=row["project_name"],
                source_object=row["source_object"],
                task_id=row["taskid"],
                app_Id=row["appid"],
                domain=row["domain"],
                source_property=row["source_property"],
                custom_expression=row["custom_expression"],
                target_property=row["target_property"],
                table_transformation=row["table_transformation"],
                tgt_data_catalog=tgt_data_catalog,
                source_path=f'{src_root_path}/{source_path}',
                target_table=f'{tgt_data_catalog}.{target_schema}.{target_table_name}',  
                archive_path=f'{archive_root_path}/{source_path.strip().lower()}/{yr}/{month}/{day}',
                formatted_datetime=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                data_layer=row["data_layer"] 
            )
            return meta

        elif row["data_layer"] == "silver":

            Metadata = namedtuple('Metadata', [
                'project_name', 'source_object', 'task_id', 'app_Id', 'domain', 'custom_expression', 'watermark_value', 
                'merge_keys', 'load_type', 'update_set', 'table_transformation', 'source_table', 'target_table', 
                'order_columns', 'merge_expr', 'data_layer', 'cdc_read'
            ])

            source_property = row["source_property"]
            source_dict = json.loads(source_property)
            src_data_catalog = source_dict['catalog']
            source_schema = source_dict['schema']
            source_table_name = source_dict['table']

            target_property = row["target_property"]
            target_dict = json.loads(target_property)
            tgt_data_catalog = target_dict['catalog']
            target_schema = target_dict['schema']
            target_table_name = target_dict['table']

            meta = Metadata(
                project_name=row["project_name"],
                source_object=row["source_object"],
                task_id=row["taskid"],
                app_Id=row["appid"],
                domain=row["domain"],
                custom_expression=row["custom_expression"],
                watermark_value=row["watermark_value"],
                merge_keys=target_dict.get("merge_keys"),
                load_type=row["load_type"],
                update_set=target_dict.get("update_set"),
                table_transformation=row["table_transformation"],
                source_table=f'{src_data_catalog}.{source_schema}.{source_table_name}',
                cdc_read=source_dict.get("cdc_read"),
                target_table=f'{tgt_data_catalog}.{target_schema}.{target_table_name}',
                order_columns=target_dict.get("order_columns"),
                merge_expr=target_dict.get("merge_expr"),
                data_layer=row["data_layer"] 
            )
            return meta

        else:
            raise ValueError("Invalid target data layer")

    except Exception as ex:
        raise Exception(f"get metadata function failed with exception: {ex}")

# COMMAND ----------

# DBTITLE 1,get_metadatadetails
def get_metadata(module_id):
    """
    Retrieves metadata details for module given by executing a SQL query.
    Parameters:
        module_id : int
            The ID of the module for which metadata details are required.
    Returns:
        pyspark.sql.DataFrame
        PySpark DataFrame containing the metadata details for the given module_id.
    """
    try:
        con, token, access_token, azure_sql_url = get_db_connection_and_token(client_id, client_secret, tenant_id)
        con.close()
        df_metadata = spark.read \
            .format("com.microsoft.sqlserver.jdbc.spark") \
            .option("url", azure_sql_url) \
            .option("dbtable", f"fwk_meta.GetMetadataDetails({module_id})") \
            .option("accessToken", access_token) \
            .option("encrypt", "true") \
            .option("hostNameInCertificate", "*.database.windows.net") \
            .load()
        return df_metadata
    except Exception as ex:
                raise Exception(f"get metadata function failed with exception: {ex}")


# COMMAND ----------

# DBTITLE 1,read_data
def read_data(blob_dir_path, config_props):
    """
    Reads data from a source given along with the configuration properties.
    Parameters:
        blob_dir_path : str
            The path of the directory in the blob storage containing the data files. 
        config_props : str
            A JSON string containing the configuration properties: source_format,config_props,source_query,source_type, partition_req

    """
    try:
        # Parse the configuration and source_format, source_query
        source_dict = json.loads(config_props)
        source_format = source_dict['format']
        config = source_dict['config']
        source_query = source_dict['query']
        source_type = source_dict['type']

        if source_type == "file" and blob_dir_path:
            blob_files_paths_list = get_file_list(blob_dir_path)
            
            # Read the data
            df = (spark.read.format(source_format)
                .options(**config)
                .load(blob_files_paths_list))

            # Add ingested_timestamp
            df = df.withColumn("ingested_timestamp", current_timestamp())\
                    .withColumn("ingested_by", current_user())

        elif source_type == "table" and source_query:
            df = spark.sql(source_query)
        elif source_type == "table" and blob_dir_path:
            df = spark.table(blob_dir_path)
        else:
            print("pass the params correctly. path or query parameter not working")
            raise Exception("issue with Params")
        return df
    except Exception as ex:
                raise Exception(f"read function failed with exception: {ex}")


# COMMAND ----------

# DBTITLE 1,Column_validation
def validate_columns(df, table_transformation):
    """
    Check if all required columns are present in the DataFrame.

    Parameters:
    - df: The DataFrame to check.
    - required_columns: A set of column names that should be present in the DataFrame.

    Returns:
    - True if all required columns are present, False otherwise.
    
    """
    try:
        # Parse the JSON string into a Python dictionary
        data = json.loads(table_transformation)

        # Extract the list of transformations
        transformations = data.get('transformations', [])

        # Extract ColumnName values
        column_names = [transformation.get('ColumnName').upper() for transformation in transformations]
        required_columns = set(column_names)
        # Get the actual columns present in the DataFrame
        actual_columns = set([x.upper() for x in df.columns])
        # Check if all required columns are in the DataFrame
        # print(required_columns - actual_columns)
        return required_columns.issubset(actual_columns)
    except Exception as ex:
                print("raising an exception")
                raise Exception(f"validate column failed function failed with exception: {ex}")

# COMMAND ----------

# DBTITLE 1,write_data
def write_to_bronze(df, target_table,target_property):
    """
    Writes to the bronze table in Delta format, with optional partitioning.
    Parameters:
        df : pyspark.sql.DataFrame
            DataFrame to be written to the bronze layer.
        target_table : str
            The name of the target table where the data will be saved.
        target_property : str
            A JSON string containing properties related to the target table, including partition columns.
        target_path : str
            The path in the data lake where the target table's data will be stored.
    """
    try:
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        current_day = now.day
        current_time = datetime.utcnow().strftime("%H%M")

        if target_property:
            target_dict = json.loads(target_property)
            partition_columns = target_dict.get('partition_column', None)
            
            if partition_columns == 'year, month, day':
                # Partition by year, month, and day
                df = (df.withColumn("year", year("ingested_timestamp"))
                        .withColumn("month", month("ingested_timestamp"))
                        .withColumn("day", dayofmonth("ingested_timestamp")))

                df.write.format("delta") \
                    .partitionBy("year", "month", "day") \
                    .option("mergeSchema", "false") \
                    .mode("append") \
                    .saveAsTable(target_table)

            elif partition_columns:
                
                # If user has passed a list of columns, partition by those columns
                partition_columns_list = partition_columns.split(', ')
                df.write.format("delta") \
                    .partitionBy(*partition_columns_list) \
                    .option("mergeSchema", "false") \
                    .mode("append") \
                    .saveAsTable(target_table)
            
            else:
                # If no partition columns are specified, write without partitioning
                df.write.format("delta") \
                    .option("mergeSchema", "false") \
                    .mode("append") \
                    .saveAsTable(target_table)

    except Exception as ex:
        raise Exception(f"write_to_bronze function failed with exception: {ex}")

# COMMAND ----------

# DBTITLE 1,run_parallel_bronze
def run_parallel_bronze(meta):
    """
    Executes a data ingestion process in parallel, handling audit logging, data transformation, 
    and writing to the bronze layer.

    Parameters:
    m : object
        Containing the necessary metadata and configuration for the task

    Returns:
        str: Returns "success" if the process completes successfully, or handles exceptions if errors occur.
    """

    try:
        spark.sparkContext.setJobDescription(f"Processing table: bronze_sap_hana.{meta.source_object}")
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", f"bronze_sap_hana.{meta.source_object}")
        task_audit_id = insert_audit_log(module_audit_id, meta.data_layer,meta.task_id,meta.app_Id)        
        if get_file_list(meta.source_path):
            #print(meta.source_path,meta.source_property)
            transform_data = read_data(meta.source_path,meta.source_property)
            if validate_columns(transform_data,meta.table_transformation):
                df_transformed=apply_custom_expression(transform_data,meta.custom_expression,meta.table_transformation)
                write_to_bronze(df_transformed,meta.target_table,meta.target_property)
                raw_to_archieve(meta.source_path,meta.archive_path)
                rows_inserted,rows_updated,rows_deleted = get_num_rows(meta.target_table)
                update_audit_log(task_audit_id, "null", "null", rows_inserted, "0", "0", "success")
                return meta.target_table,"Success"
            else:
                
                update_audit_log(task_audit_id, "null", "missing columns in source", "0", "0", "0", "failed")
                return meta.target_table,"Failure"
        else:
            updated_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            update_audit_log(task_audit_id, "null", "null", "0", "0", "0", f"No files found as of {updated_timestamp}")
            return meta.target_table,"No files found"
    except Exception as e:
        print(f"An error occurred: {e}")
        handle_exception(e, task_audit_id)
        return meta.target_table,"Failure"

# COMMAND ----------

# DBTITLE 1,deduplicate_data
def deduplicate_data(df, col_list, order_exprs):
    """
    Deduplicates based on the key columns and keeps the most recent record based on provided ordering expressions.

    Parameters:
        df : pyspark.sql.DataFrame
            The DataFrame to be deduplicated.
        col_list : list
            The list of key columns used to identify duplicates.
        order_exprs : list of str
            List of ordering expressions to determine the most recent record. 
            Each expression should be in the form of a string, e.g., "col_name DESC".

    Returns:
        pyspark.sql.DataFrame
        Deduplicated DataFrame containing only the latest records based on the key columns and ordering expressions.
    """
    try:
        if len(col_list) == 0:
            raise ValueError("Key Columns should be mentioned")
        
        if not isinstance(col_list, list):
            raise ValueError("Key Columns must be a list of column names")
        
        # Construct the order by expression
        order_exprs = eval(order_exprs)

        # Add a row number to each row within each partition
        df_with_row_num = df.withColumn("rn", F.expr(f"row_number() over (partition by {','.join(col_list)} order by {', '.join(order_exprs)})"))
        
        # Filter to get only the rows with row number equal to 1
        df_deduplicated = df_with_row_num.filter(F.col("rn") == 1).drop("rn")
        
        return df_deduplicated
    
    except Exception as ex:
        raise Exception(f"deduplicate function failed with exception: {ex}")

# COMMAND ----------

# DBTITLE 1,move to archieve
def raw_to_archieve(file_list, archive_path):
    """
    Moves a list of files to the specified archive path.
    
    :param file_list: List of file paths to archive.
    :param archive_path: Destination path where files should be moved.
    :raises Exception: If any file move fails, an exception is raised.
    """
    try:
        items = dbutils.fs.ls(file_list)
        for item in items:
            if not item.isDir():
                # Define the source and destination paths for the file
                file_path = item.path
                file_name = file_path.split('/')[-1]
                dest_path = f"{archive_path}/{file_name}"
                #print(file_path,file_name, dest_path)
                # Move the file to the archive path
                dbutils.fs.mv(file_path, dest_path)
                print(f"Moved file: {file_path} to {dest_path}")
    except Exception as ex:
        raise Exception(f"raw to archive function failed with exception: {ex}")

# COMMAND ----------

# DBTITLE 1,InsertAuditLogStoredProcs
def insert_audit_log(module_audit_id, data_layer, task_id, app_id):
    """
    Inserts an audit log entry for a given module in the metadata table.
    Parameters:
        module_audit_id : int
            The ID of the module audit.
        data_layer : str
            The data layer being processed.
        task_id : int
            The ID of the task associated with the audit.
        app_id : int
            The ID of the application triggering the audit.
    """
    try:  
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        statement = f"""
        DECLARE @module_audit_id INTEGER='{module_audit_id}'
        DECLARE @data_layer VARCHAR(50)='{data_layer}'
        DECLARE @task_id INTEGER = '{task_id}'
        DECLARE @app_id INTEGER = '{app_id}'
        DECLARE @start_time DATETIME = '{start_time}'
        EXEC fwk_meta.InsertTaskAuditLog  @module_audit_id ,@data_layer, @task_id , @app_id,@start_time;
        """
        con, token, access_token, azure_sql_url = get_db_connection_and_token(client_id, client_secret, tenant_id)

        exec_statement = con.prepareCall(statement)
        exec_statement.execute()
        rs = exec_statement.getResultSet()
        
        if rs.next():
            task_audit_id = rs.getInt(1)
            return task_audit_id

        exec_statement.close()
        con.close()
    except Exception as ex:
        raise Exception(f"insert_audit_log function failed with exception: {ex}")

# COMMAND ----------

# DBTITLE 1,UpdatedAuditLogStoredProcs
def update_audit_log(task_audit_id, ErrorCode, ErrorDesc, rows_inserted, rows_updated, rows_deleted, status):
   """
    Updates an existing audit log entry in the metadata table with execution details.
    Parameters:
        task_audit_id : int
            ID of the task audit log to update.
        ErrorCode : str
            If any error occurred during the task execution.
        ErrorDesc : str
            Description of the error during the task execution.
        rows_inserted : str
            Number of rows inserted.
        rows_updated : str
            Number of rows updated.
        rows_deleted : str
            Number of rows deleted.
        status : str
            The status of the task- Success and Failed.
   """
   try:
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        statement = f"""
         DECLARE @task_audit_id INTEGER='{task_audit_id}'
         DECLARE @end_time DATETIME = '{end_time}'
         DECLARE @ErrorCode VARCHAR(50)='{ErrorCode}'
         DECLARE @ErrorDesc VARCHAR(255)='{ErrorDesc}'
         DECLARE @rows_inserted VARCHAR(50)='{rows_inserted}'
         DECLARE @rows_updated VARCHAR(50)='{rows_updated}'
         DECLARE @rows_deleted VARCHAR(50)='{rows_deleted}'
         DECLARE @status VARCHAR(50)='{status}'

         EXEC fwk_meta.UpdateTaskAuditLog  @task_audit_id ,@end_time, @ErrorCode , @ErrorDesc, @rows_inserted, @rows_updated, @rows_deleted,  @status;
      """

        con, token, access_token, azure_sql_url = get_db_connection_and_token(client_id, client_secret, tenant_id)

        exec_statement = con.prepareCall(statement)
        exec_statement.execute()
        exec_statement.close()
        con.close()
      
   except Exception as ex:
        raise Exception(f"update_audit_log function failed with exception: {ex}")

# COMMAND ----------

# DBTITLE 1,update_watermark_column
def update_watermark_column(taskid, watermark_value, load_type):
   """
    Updates the watermark column in the metadata table by executing a stored procedure.

    Parameters:
    taskid : int
        The ID of the task for which the watermark is being updated.
    watermark_value : str
        The new watermark value to be set.
    load_type : str
        The type of load operation (e.g., 'full', 'incremental').
   """
   try:  
      statement = f"""
         DECLARE @taskid INTEGER='{taskid}'
         DECLARE @watermark_value NVARCHAR(max) = '{watermark_value}'
         DECLARE @load_type VARCHAR(50) = '{load_type}'

         EXEC fwk_meta.UpdateWatermarkTable  @taskid, @watermark_value, @load_type
      """
      con, token, access_token, azure_sql_url = get_db_connection_and_token(client_id, client_secret, tenant_id)

      exec_statement = con.prepareCall(statement)
      exec_statement.execute()
      exec_statement.close()
      con.close()   
   except Exception as ex:
        raise Exception(f"UpdateWatermarkTable function failed with exception: {ex}")

# COMMAND ----------

# DBTITLE 1,Transformation function - Part 1
def apply_custom_expression(df, custom_expression, transformation=None):
    """
    Applies a custom expression or transformation to a DataFrame.
    Parameters:
        df : pyspark.sql.DataFrame
            Input DataFrame to which the custom expression or transformation will be applied.
        custom_expression : str
            JSON-formatted string representing the custom expressions to apply to the DataFrame.
        transformation : optional
            Apply as a custom transformation if no custom expression is provided.
    Returns:
        pyspark.sql.DataFrame
        Transformed PySpark DataFrame after applying the custom expression or transformation.
    """
    try:
        if custom_expression:
            custom_exprs = eval(custom_expression)
            df_transformed = df.selectExpr(*custom_exprs)
            return df_transformed

        if not custom_expression and transformation:
            apply_custom_transformation(df, transformation)
        
        if not custom_expression and not transformation:
            return df
    except Exception as ex:
                raise Exception(f"custom expression function failed with exception: {ex}")

# COMMAND ----------

# DBTITLE 1,Transformation function - Part 2
def apply_custom_transformation(df, transformation):
    """
    Applies custom transformations to a DataFrame based on the transformation configuration.
    Parameters:
        df : pyspark.sql.DataFrame
            The input DataFrame to which the transformations will be applied.
        transformation : str
            A JSON-formatted string that specifies the transformations to apply.
    Returns:
        pyspark.sql.DataFrame
            Transformed PySpark DataFrame with the given transformations applied.

    """
    try:
        exprs = []
        transformations = json.loads(transformation)

        for transform in transformations["transformations"]:
            column_name = transform["ColumnName"]
            data_type = transform["DataType"].lower()
            format_option = transform.get("Format", None)
            alias = transform.get("ColumnAlias", column_name)

            # Define expression based on data type and format
            if data_type == "string":
                if format_option == "upper":
                    expr = f"UPPER({column_name})"
                elif format_option == "lower":
                    expr = f"LOWER({column_name})"
                else:
                    expr = f"CAST({column_name} AS STRING)"

            elif data_type == "date":
                if format_option:
                    expr = f"TO_DATE({column_name}, '{format_option}')"
                else:
                    expr = f"CAST({column_name} AS DATE)"

            elif data_type == "float":
                expr = f"CAST({column_name} AS FLOAT)"

            elif data_type == "int":
                expr = f"CAST({column_name} AS INT)"

            elif data_type == "timestamp":
                if format_option:
                    expr = f"TO_TIMESTAMP({column_name}, '{format_option}')"
                else:
                    expr = f"CAST({column_name} AS TIMESTAMP)"

            elif data_type == "bigint":
                expr = f"CAST({column_name} AS BIGINT)"

            elif data_type == "double":
                expr = f"CAST({column_name} AS DOUBLE)"

            elif data_type == "boolean":
                expr = f"CAST({column_name} AS BOOLEAN)"

            # Apply aliasing
            if alias and alias != column_name:
                expr = f"{expr} AS `{alias}`"
            else:
                expr = f"{expr} AS `{column_name}`"

            exprs.append(expr)

        # Apply transformations to the DataFrame
        df_transformed = df.selectExpr(*exprs)
        return df_transformed
    except Exception as ex:
                raise Exception(f"custom transformation function failed with exception: {ex}")

# COMMAND ----------

# DBTITLE 1,convert_to_list
def convert_to_list(string):
    """
    Converts a comma-separated string into a list of strings.
    Parameters:
        string : str
            The comma-separated string to convert.
    Returns:
        list: A list of strings.
    """
    try:
        li = list(string.split(","))
        return li
    except Exception as e:
        print(f"An error occurred: {e}")

# COMMAND ----------

# DBTITLE 1,batchdf
def transform_data(source_table_name, last_ingested_timestamp, merge_keys, custom_expression, table_transformation, order_columns, load_type, cdf=True):
    """
        Processes and transforms data from a specified source table based on the last ingested timestamp.
        
        Parameters:
            source_table_name : str
                Name of the source table.
            last_ingested_timestamp : str
                Timestamp to filter records that are more recent.
            merge_keys : str
                Used to identify and match records for merging operations.
            custom_expression : str
                JSON string representing custom expressions to apply to the DataFrame.
            table_transformation : str
                JSON string representing additional table transformations to apply.
            order_columns : list of str
                List of columns to order by during deduplication.
            load_type : str
                Type of load, either 'full' or 'incremental'.
            cdf : bool
                Whether to use Change Data Feed (CDF).
        
        Returns:
            pyspark.sql.DataFrame, int, int
            Returns the filtered DataFrame, start_version, and end_version.
            Returns None if no updates are found or if start_version == end_version.
    """
    if cdf:
        # Set the start version based on the last_ingested_timestamp
        start_version = 0 if int(last_ingested_timestamp) == 19000101 else int(last_ingested_timestamp)
        details = spark.sql(f"DESCRIBE HISTORY {source_table_name} LIMIT 1").select("version", "operation").collect()
        operation_type = details[0].operation
        end_version = details[0].version

        if load_type == 'full':
            upd_end_version = end_version if operation_type!= 'OPTIMIZE' else end_version-1
            if start_version < upd_end_version:
                start_version = upd_end_version
            else:
                return None, start_version, end_version

        elif load_type == 'incremental' or load_type == 'delete-insert':
            if start_version >= end_version:
                return None, start_version, end_version
            else:
                start_version = start_version+1

        else:
            raise Exception("Invalid load_type specified.")

        print(f'{source_table_name}: Startversion: {start_version}, Endversion: {end_version}')

        filtered_source_df = spark.read \
            .option("readChangeFeed", "true") \
            .option("startingVersion", start_version) \
            .option("endingVersion", end_version) \
            .table(source_table_name)

        filtered_source_df = deduplicate_data(filtered_source_df, convert_to_list(merge_keys), order_columns)
        filtered_source_df = apply_custom_expression(filtered_source_df, custom_expression, table_transformation)
        return filtered_source_df, start_version, end_version

    else:
        source_df = spark.table(source_table_name)
        filtered_source_df = source_df.filter(F.col("extracted_timestamp") > last_ingested_timestamp)
        filtered_source_df = deduplicate_data(filtered_source_df, convert_to_list(merge_keys), order_columns)
        filtered_source_df = apply_custom_expression(filtered_source_df, custom_expression, table_transformation)
        return filtered_source_df, None, None

# COMMAND ----------

# DBTITLE 1,run_parallel_silver
def run_parallel_silver(meta):
    """
        Executes a data ingestion process in parallel for the silver layer, handling audit logging,
        data transformation.
        
        Parameters:
            m : object
                An object containing the necessary metadata and configuration for the task, including:
        Returns:
            str: Returns "success" if the process completes successfully, or handles exceptions if errors occur.
    """
    try:
        spark.sparkContext.setJobDescription(f"Processing table: silver_sap_hana.{meta.source_object}")
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", f"silver_sap_hana.{meta.source_object}")
        task_audit_id = insert_audit_log(module_audit_id, meta.data_layer, meta.task_id, meta.app_Id)
        filtered_source_df,start_version,end_version = transform_data(source_table_name=meta.source_table, last_ingested_timestamp = meta.watermark_value,
                       merge_keys=meta.merge_keys,custom_expression=meta.custom_expression, table_transformation=meta.table_transformation,order_columns=meta.order_columns, load_type=meta.load_type,cdf=meta.cdc_read)

        if filtered_source_df is not None:
            write_to_silver(merge_strategy=meta.load_type,merge_expr=meta.merge_expr,source_df=filtered_source_df, target_table_name=meta.target_table, update_set = meta.update_set,last_ingested_timestamp = meta.watermark_value) 
            rows_inserted, rows_updated, rows_deleted = get_num_rows(meta.target_table) 
            update_audit_log(task_audit_id, "null", "null", rows_inserted, rows_updated, rows_deleted, "success")
            # print(f'{meta.target_table} processed version: {end_version}')
            update_watermark_column(meta.task_id,end_version,meta.load_type)
            return meta.target_table, start_version, end_version, "Success"
        else:
            update_audit_log(task_audit_id, "null", "null","0", "0", "0", "skipped:no new version")  
            return meta.target_table, start_version, end_version, "skipped:no new version"

    except Exception as e:
            print(f"An error occurred: {e}")
            handle_exception(e, task_audit_id)
            return meta.target_table, start_version, end_version, "Failure"

# COMMAND ----------

# DBTITLE 1,write_to_silver
def write_to_silver(merge_strategy, merge_expr, source_df, target_table_name, update_set, last_ingested_timestamp):
    """
        Writes data to a Delta table in the Silver layer in full and incremental merge strategies.
        Parameters:
            merge_strategy : str
                The strategy for merging data into the target table. Can be "full" or "incremental".
            merge_expr : str
                The merge condition used in the Delta merge operation.
            source_df : DataFrame
                The DataFrame containing the source data to be written.
            target_table_name : str
                The name of the target Delta table.
            update_set : bool
                A flag indicating whether to perform updates in the incremental merge strategy.
            last_ingested_timestamp : str
    """
    try:
        delta_table = DeltaTable.forName(spark, target_table_name)
        
        source_df = source_df.withColumn("ingested_timestamp", current_timestamp())\
                    .withColumn("ingested_by", current_user())
                    
        if merge_strategy == "full":
            write_mode = "overwrite"
            source_df.write.format("delta").mode(write_mode).option("mergeSchema", "false").saveAsTable(target_table_name)
        
        elif merge_strategy == "incremental":
            merge_condition = F.expr(merge_expr)
            update_clause = {col: f"source.{col}" for col in source_df.columns}
            insert_clause = {col: f"source.{col}" for col in source_df.columns}

            if update_set:
                # Perform update and insert in case of a match or no match
                delta_table.alias("target").merge(
                    source_df.alias("source"),
                    merge_condition
                ).whenMatchedUpdate(set=update_clause).whenNotMatchedInsert(values=insert_clause).execute()
            else:
                # Only perform insert in case of no match
                delta_table.alias("target").merge(
                    source_df.alias("source"),
                    merge_condition
                ).whenNotMatchedInsert(values=insert_clause).execute()

        elif merge_strategy=="delete-insert":
            update_clause = {col: f"source.{col}" for col in source_df.columns}
            insert_clause = {col: f"source.{col}" for col in source_df.columns}

            if len(merge_expr.split(';'))==3:
                merge_condition = F.expr(merge_expr.split(';')[0])
                watermark_col = convert_to_list(merge_expr.split(';')[1])
                scan_tenure = int(merge_expr.split(';')[2])
                if len(watermark_col) == 1:
                    max_exprs = F.max(watermark_col[0])
                else:
                    max_exprs = F.greatest(*[F.max(col) for col in watermark_col])
            else:
                raise Exception("Invalid input format. Expected format: 'delete-insert;watermark_col;scan_tenure'")

            max_date_df = source_df.agg(max_exprs.alias("max_date"))
            max_date_df = max_date_df.withColumn("min_date", F.date_sub(F.col("max_date"), scan_tenure))

            date_data = max_date_df.collect()
            if date_data:
                max_date=date_data[0]['max_date']
                min_date = date_data[0]['min_date']
            else:
                raise Exception("No data found in the source df")

            if max_date and min_date:
                delete_stmt=" or ".join([f"target.{x} between '{min_date}' and '{max_date}'" for x in watermark_col])
                delete_clause=F.expr(delete_stmt)
                delta_table.alias("target").merge(
                    source_df.alias("source"),
                    merge_condition
                ).whenMatchedUpdate(set=update_clause)\
                    .whenNotMatchedInsert(values=insert_clause)\
                    .whenNotMatchedBySourceDelete(condition=delete_clause)\
                    .execute()
            else:
                raise Exception("Invalid input format. Expected format: 'delete-insert;watermark_col;scan_tenure'")

    except Exception as ex:
        raise Exception(f"Error processing in silver layer: {ex}")
        return 0

# COMMAND ----------

# DBTITLE 1,get_latest_rows_inserted
def get_num_rows(table_name):
    """
    Retrieves the number of rows inserted, updated, and deleted.
    Parameters:
        table_name : str
            The name of the Delta table for which to retrieve row metrics.
    Returns:
        tuple: A tuple containing rows_inserted,rows_updated,rows_deleted:

    """
    try:
        df_history = spark.sql(f"DESCRIBE HISTORY {table_name}").limit(1)
        operation_metrics = df_history.select("operationMetrics").collect()[0]["operationMetrics"]
        if 'numTargetRowsInserted' in str(operation_metrics):
            rows_inserted = int(operation_metrics.get("numTargetRowsInserted", 0))
        else:
            rows_inserted = int(operation_metrics.get("numOutputRows", 0))
        rows_updated = int(operation_metrics.get("numTargetRowsUpdated", 0))
        rows_deleted = int(operation_metrics.get("numTargetRowsNotMatchedBySourceDeleted", 0))

        return rows_inserted, rows_updated, rows_deleted
    except Exception as ex:
        raise Exception(f"get_num_rows function failed with exception: {ex}")

# COMMAND ----------

# DBTITLE 1,Error handling
def handle_exception(e, task_audit_id):
    """
    Handles exceptions by updating the audit log with the error information.
    Parameters:
        e : Exception
            The exception object to handle.
        task_audit_id : int
            The ID of the task being audited, used to update the audit log.
    """ 
    is_pyspark_exception = issubclass(type(e), PySparkException)
    error_class=''
    message_parameters =''
    sqlstate=''
    if is_pyspark_exception:
            print("inside pyspark exception")
            error_class : e.getErrorClass() 
            message_parameters = str(e.getMessageParameters())
            sqlstate = e.getSqlState()
            error_code = f"{sqlstate}"
            if error_code == "None" or error_code == '':
                error_code = "UNKNOWN"
            error_message=f"{error_class}{message_parameters}{sqlstate}{e}"
            error_message=str(error_message).replace("'","")
            update_audit_log(task_audit_id, error_code, error_message, "0", "0", "0", "failed")
    else:
            print("not inside pyspark exception")
            error_message=str(e).replace("'","")
            error_code = f"{sqlstate}"
            if error_code == "None" or error_code == '':
                error_code = "UNKNOWN"
            update_audit_log(task_audit_id, error_code, error_message, "0", "0", "0", "failed")

# COMMAND ----------

# DBTITLE 1,Print Status
print('delta Write optimizer enabled...')
print('secrets imported from keyvault...')
print('spark config with blob - set...')
print("file identifier - set. Use the fn 'get_file_list(blob_dir_path)' to scan files recursively...")
