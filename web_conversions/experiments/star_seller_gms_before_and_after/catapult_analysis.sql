------------------------------------------------------------------------------------------------------------------------
-- GET ALL BUCKETED BROWSERS 
------------------------------------------------------------------------------------------------------------------------
DECLARE config_flag_param STRING DEFAULT "growth_regx.sh_shop_info_banner_redesign_v3_desktop";
DECLARE start_date DATE; -- DEFAULT "2023-08-22";
DECLARE end_date DATE; -- DEFAULT "2023-09-04";
DECLARE is_event_filtered BOOL; -- DEFAULT FALSE;
DECLARE bucketing_id_type INT64;

IF start_date IS NULL OR end_date IS NULL then
    SET (start_date, end_date) = (
        SELECT AS STRUCT
            MAX(DATE(boundary_start_ts)) AS start_date,
            MAX(_date) AS end_date,
        FROM
            `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
            experiment_id = config_flag_param
    );
END IF;

IF is_event_filtered IS NULL then
    SET (is_event_filtered, bucketing_id_type) = (
        SELECT AS STRUCT
            is_filtered,
            bucketing_id_type,
        FROM
            `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
            _date = end_date
            AND experiment_id = config_flag_param
    );
ELSE
    SET bucketing_id_type = (
        SELECT
            bucketing_id_type,
        FROM
            `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
            _date = end_date
            AND experiment_id = config_flag_param
    );
END IF;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` AS ( -- find the 
select 
  _date,
  variant_id,
  bucketing_ts, 
  bucketing_id,
  experiment_id,
from
  `etsy-data-warehouse-prod.catapult_unified.bucketing`
where
  experiment_id = config_flag_param
);

------------------------------------------------------------------------------------------------------------------------
-- LOOK AT SHOP HOME TRAFFIC FOR ALL VISITS 
------------------------------------------------------------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.holder_table as (
select
   fb._date as bucket_date
  , bm._date as visit_date
  , browser_id
  , variant_id
  , (select kv.value from unnest(properties.map) as kv where kv.key = "shop_shop_id") as shop_id
  , count(*) as visits 
from 
  etsy-visit-pipe-prod.canonical.beacon_main_2025_06 bm-- june data
inner join 
  `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` fb 
    on fb.bucketing_id=bm.browser_id
where 1=1
  and event_name = "shop_home"
  and bm._date >=('2025-06-16') and bm._date <=('2025-06-24')
  and primary_event is true
group by 1,2,3,4,5 
); 
