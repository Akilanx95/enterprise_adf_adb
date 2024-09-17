SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
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
            updatedby = ISNULL(@updatedby, SYSTEM_USER)
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
