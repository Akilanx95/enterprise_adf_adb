SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
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
