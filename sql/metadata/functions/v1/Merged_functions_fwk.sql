-- Function for the GetMetadataDetails based on Module_id & GetProjectMetadataDetails

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

 CREATE    FUNCTION [fwk_meta].[GetMetadataDetails]
(
    @module_id INTEGER
)
RETURNS TABLE
AS
RETURN
(SELECT 	
            p.project_name as project_name,
            m.module_id AS moduleid,
            m.data_layer as data_layer,
            es1.source_name AS source_name,
            e2.load_type as load_type,
			es1.source_type AS data_source,
			es1.source_property AS source_connection_details,--credenetials for linked services and datasets
			e1.entity_name AS source_object,
			e1.entity_property as source_property, -- selection criteria and watermark columns
			es2.source_name AS target_name,
			es2.source_type AS target_type,
			es2.source_property AS target_connection_details,--credenetials for linked services and datasets
			e2.entity_name AS target_object,
			e2.entity_property AS target_property, -- target location and partition pattern
            e2.domain AS domain,
            e2.batch_id as batch_id,
            t.task_id AS taskid,
			t.task_property AS table_properties,
			t.watermark_value AS watermark_value,
            a.app_id AS appid,
			a.app_name AS app_name,
			a.app_type AS app_type,
			a.app_property AS app_property,
            tr.table_transformation AS table_transformation,
            tr.custom_expression AS custom_expression
	FROM fwk_meta.module m 
    JOIN fwk_meta.project p ON m.project_id=p.project_id
	JOIN fwk_meta.task t ON m.module_id = t.module_id
	JOIN fwk_meta.entity e1 ON t.source_entity=e1.entity_id
	JOIN fwk_meta.entity e2 ON t.target_entity=e2.entity_id
	JOIN fwk_meta.entity_source es1 ON e1.entity_source_id=es1.entity_source_id
	JOIN fwk_meta.entity_source es2 ON e2.entity_source_id=es2.entity_source_id
	LEFT JOIN fwk_meta.app a On t.app_id = a.app_id
    LEFT JOIN fwk_meta.transformation tr on tr.entity_id=e2.entity_id
	WHERE m.module_id=@module_id
	AND m.is_active_flag=1
    AND t.is_active_flag=1
    AND e1.is_active_flag=1
    AND a.is_active_flag=1
    AND es1.is_active_flag=1
	)
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
