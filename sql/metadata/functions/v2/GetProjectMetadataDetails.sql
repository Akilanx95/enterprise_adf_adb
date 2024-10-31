SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
 CREATE   FUNCTION [fwk_meta].[GetProjectMetadataDetails]
(
    @ProjectID INTEGER
)
RETURNS TABLE
AS
RETURN
(SELECT 	m.module_id AS moduleid,
            m.data_layer as data_layer,
            m.priority as priority
	FROM fwk_meta.module m 
	JOIN fwk_meta.project p
    ON m.project_id=p.project_id
    WHERE p.project_id=@ProjectID
	)
GO
