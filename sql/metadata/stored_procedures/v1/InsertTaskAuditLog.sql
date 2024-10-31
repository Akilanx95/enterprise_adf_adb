SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
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
