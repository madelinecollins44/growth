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
with agg as (
select
   min(fb._date) as first_bucket_date
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
  and event_name = "shop_home" -- only shop home traffic 
  and bm._date >=('2025-06-16') and bm._date <=('2025-06-24') -- during experiment run 
  -- and primary_event is true 
group by all 
)
select * from agg where visit_date >= first_bucket_date -- only looking at visits that happened post bucketing visit 
); 

-- select variant_id, count(distinct browser_id) from etsy-data-warehouse-dev.madelinecollins.holder_table  group by 1

------------------------------------------------------------------------------------------------------------------------
-- ADDING IN TRANSACTION DATA TO TRAFFIC 
------------------------------------------------------------------------------------------------------------------------
with trans as (
select
  _date, 
  split(visit_id, ".")[0] as browser_id, 
  shop_id,
  sum(trans_gms_net) as trans_gms_net,
  count(distinct transaction_id) as transactions
from 
  etsy-data-warehouse-prod.visit_mart.visits_transactions vt
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics sb 
    on vt.seller_user_id=sb.user_id
where 1=1
  and transaction_live=1 -- trans is still live
  and _date >=('2025-06-16') and _date <=('2025-06-24') -- dates of experiment 
group by 1,2,3
)
, traffic as (
select
  variant_id,
  visit_date,
  browser_id, 
  ht.shop_id,
  case when ssd.shop_id is not null then 1 else 0 end as star_seller_status,
  sum(visits) as total_visits
from 
  etsy-data-warehouse-dev.madelinecollins.holder_table ht
left join 
    (select
      distinct shop_id 
    from 
      etsy-data-warehouse-prod.star_seller.star_seller_daily 
    where 1=1
      and (_date >= ('2025-06-16') and _date <=('2025-06-24'))
      and is_star_seller is true) ssd 
  on cast(ssd.shop_id as string)=ht.shop_id
group by 1,2,3,4,5
)
select
  -- count(distinct tfc.browser_id) as browsers,
  -- count(distinct tfc.shop_id) as shops,
  -- sum(tfc.total_visits) as total_visits
  -- variant_id,
  coalesce(star_seller_status,0) as star_seller_status,
  count(distinct tfc.browser_id) as browser_visits,
  count(distinct tfc.shop_id) as shop_visits,
  sum(total_visits) as total_visits,
  count(distinct trns.browser_id) as browser_converts,
  count(distinct trns.shop_id) as shop_converts,
  sum(transactions) as total_transactions,
  sum(trans_gms_net) as total_gms,
from 
  traffic tfc
left join 
  trans trns 
    on tfc.browser_id=trns.browser_id
    and cast(trns.shop_id as string)=tfc.shop_id
    -- and tfc._date=date(timestamp_seconds(_date))
group by 1
order by 2,1 desc
