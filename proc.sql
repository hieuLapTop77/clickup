USE [db_test]
GO

/****** Object:  StoredProcedure [dbo].[sp_Custom_Fields_List]    Script Date: 7/10/2024 8:18:06 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [dbo].[sp_Custom_Fields_List] AS
-- ===============================================
--
-- ===============================================
IF OBJECT_ID('tempdb..#tmp_list') IS NOT NULL DROP TABLE #tmp_list;
with tmp as ( --58
SELECT id,name,status,custom_fields,date_created
FROM [db_test].[dbo].[3rd_clickup_task_details]
where  JSON_VALUE(list, '$.id')  in (901802184458,901802268429,901802268438,901802276882,901802290640,901802287142,901802287144,901802300868,901802301544)  --dây là danh sách task c?n l?y
),
tmp_value as (
select jd.id,name,jd.status,jd.custom_fields,jd.date_created,
JSON_VALUE(value, '$.id') AS id_custom_fields,
JSON_VALUE(value, '$.name') AS name_custom_fields,
JSON_VALUE(value, '$.value') AS value_custom_fields,
JSON_VALUE(value, '$.type') AS type_custom_fields,
value value_list
from tmp jd
CROSS APPLY OPENJSON(jd.custom_fields) AS value
)
select distinct value_list ,JSON_VALUE(value_list, '$.id') AS id,JSON_VALUE(value_list, '$.name') AS name,JSON_VALUE(value_list, '$.value') AS value
into #tmp_list
from tmp_value jd
where JSON_VALUE(value_list, '$.type')  = 'drop_down';

IF OBJECT_ID('tempdb..#tmp_drop_down_value') IS NOT NULL DROP TABLE #tmp_drop_down_value;
select distinct 
id,name,
--jd.value,
JSON_VALUE(custom_fields_value.value, '$.name') AS name_value,
JSON_VALUE(custom_fields_value.value, '$.orderindex') AS orderindex_value
into #tmp_drop_down_value
from #tmp_list jd
CROSS APPLY OPENJSON(jd.value_list, '$.type_config.options') AS custom_fields_value
;

truncate table [3rd_misa_drop_down_value]
insert into [3rd_misa_drop_down_value](id,name_drop_down,name_value,orderindex_value)
select id,name,name_value,orderindex_value
from #tmp_drop_down_value;
GO


