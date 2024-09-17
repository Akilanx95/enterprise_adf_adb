-- Stored Procedure for Inserting Audit log's to module_audit table

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE    PROCEDURE [fwk_meta].[InsertModuleAuditLog]
    @moduleid INTEGER,
	@start_time DATETIME,
    @data_layer VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @module_audit_id INTEGER;
    
    BEGIN TRY
        BEGIN TRANSACTION;

        -- Insert data into module level audit table
        INSERT INTO fwk_meta.module_audit (module_id, data_layer, status, start_time,created_time,createdby,updatedby)
        VALUES (@moduleid, @data_layer,'In progress',@start_time,@start_time,SYSTEM_USER,SYSTEM_USER)

		-- retrive inserted module_audit_id which is used for update
		SET  @module_audit_id = SCOPE_IDENTITY();
		
        -- Commit the transaction if all steps succeed
        COMMIT TRANSACTION;

        --returning module_audit_id as part of result_set
        SELECT @module_audit_id as module_audit_id

    END TRY
    BEGIN CATCH
        -- If an error occurs, rollback the transaction
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Raise an error to return to the calling code
        THROW;

    END CATCH;
END
GO


-- Stored Procedure for Inserting Audit log's to task_audit table
CREATE   PROCEDURE [fwk_meta].[InsertTaskAuditLog]
    @module_audit_id INTEGER,
    @data_layer VARCHAR(50),
    @task_id INTEGER,
    @app_id INTEGER,
    @start_time DATETIME   
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @task_audit_id INTEGER;
    
    BEGIN TRY
        BEGIN TRANSACTION;

        -- Insert data into module level audit table
        INSERT INTO fwk_meta.task_audit (module_audit_id, data_layer, status,task_id,app_id, start_time,created_time,createdby,updatedby)
        VALUES (@module_audit_id, @data_layer,'In progress', @task_id, @app_id, @start_time,@start_time,SYSTEM_USER,SYSTEM_USER)

		-- retrive inserted module_audit_id which is used for update
		SET  @task_audit_id = SCOPE_IDENTITY();
		
        -- Commit the transaction if all steps succeed
        COMMIT TRANSACTION;

        --returning module_audit_id as part of result_set
        SELECT @task_audit_id as task_audit_id

    END TRY
    BEGIN CATCH
        -- If an error occurs, rollback the transaction
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Raise an error to return to the calling code
        THROW;

    END CATCH;
END
GO


-- Stored Procedure for Updating Audit log's to module_audit table
CREATE PROCEDURE [fwk_meta].[UpdateModuleAuditLog]
    @module_audit_id INTEGER,
	@end_time DATETIME,
	@ErrorCode VARCHAR(50),
	@ErrorDesc VARCHAR(255),
	@status VARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;
	
	 DECLARE @JsonError NVARCHAR(MAX);
    
    BEGIN TRY
        BEGIN TRANSACTION;
		
		SET @JsonError = '{"ERROR_CODE":"' + @ErrorCode + '","ERROR_DESC":"' + @ErrorDesc + '"}';

        -- update data into audit table
        UPDATE fwk_meta.module_audit
		SET end_time=@end_time,
			status=@status,
			module_metrics=@JsonError,
            updated_time=@end_time,
            updatedby = SYSTEM_USER
		WHERE module_audit_id = @module_audit_id
		
		UPDATE fwk_meta.module_audit
		SET total_time = DATEDIFF(MINUTE, start_time, end_time)
		WHERE module_audit_id = @module_audit_id
		
		
        -- Commit the transaction if all steps succeed
        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH
        -- If an error occurs, rollback the transaction
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Raise an error to return to the calling code
        THROW;

    END CATCH;
END
GO


-- Stored Procedure for Updating Audit log's to task_audit table
CREATE   PROCEDURE [fwk_meta].[UpdateTaskAuditLog]
    @task_audit_id INTEGER,
	@end_time DATETIME,
	@ErrorCode NVARCHAR(max),
	@ErrorDesc NVARCHAR(max),
    @rows_inserted VARCHAR(50),
    @rows_updated VARCHAR(50) ,
    @rows_deleted VARCHAR(50) ,
	@status VARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;
	
	 DECLARE @task_metrics NVARCHAR(MAX);
    
    BEGIN TRY
        BEGIN TRANSACTION;
		
		SET @task_metrics = '{"ROWS_INSERTED":"' + @rows_inserted + '","ROWS_UPDATED":"' + @rows_updated + '","ROWS_DELETED":"' + @rows_deleted + '",
                            "ERROR_CODE":"' + @ErrorCode + '","ERROR_DESC":"' + @ErrorDesc + '"}';

        -- update data into audit table
        UPDATE fwk_meta.task_audit
		SET end_time=@end_time,
			status=@status,
			task_metrics=@task_metrics,
            updated_time=@end_time,
            updatedby = SYSTEM_USER
		WHERE task_audit_id = @task_audit_id
		
		UPDATE fwk_meta.task_audit
		SET total_time = DATEDIFF(MINUTE, start_time, end_time)
		WHERE task_audit_id = @task_audit_id
		
		
        -- Commit the transaction if all steps succeed
        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH
        -- If an error occurs, rollback the transaction
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Raise an error to return to the calling code
        THROW;

    END CATCH;
END
GO


-- Stored Procedure for Updating watermark value in task table
CREATE   PROCEDURE [fwk_meta].[UpdateWatermarkTable]
    @taskid INTEGER,
    @watermark_value VARCHAR(100),
    @load_type VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;

        -- Update fwk_meta.task with the new watermark_value
        UPDATE fwk_meta.task
        SET watermark_value = @watermark_value
        WHERE task_id = @taskid
        AND @load_type='incremental';

        -- Commit the transaction if all steps succeed
        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH
        -- If an error occurs, rollback the transaction
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Raise an error to return to the calling code
        THROW;
    END CATCH;
END
GO
