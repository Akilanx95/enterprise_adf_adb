SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
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
