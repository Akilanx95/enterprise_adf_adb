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
