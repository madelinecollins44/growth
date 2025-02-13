----------------------------------------------------------------------
-- OVERALL COUNTS TO CONFIRM
----------------------------------------------------------------------
--overall gms from last 30 days (all platforms, just web)
select
  sum(gms_net) as gms_from_visit
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans g
-- inner join 
--   etsy-data-warehouse-prod.transaction_mart.transactions_visits using (transaction_id)
where 
  g.date >= current_date-30 
  -- and platform_app in ('mobile_web','desktop')
group by all 

--overall visits / conversion 
  select
  count(distinct v.visit_id) as total_traffic,
  count(distinct case when platform in ('mobile_web','desktop') then visit_id end) as web_traffic,
  count(distinct case when platform in ('mobile_web','desktop') and event_type in ('shop_home') then visit_id end) as sh_web_traffic,
  count(distinct case when event_type in ('shop_home') then visit_id end) as sh_traffic,
  count(distinct case when converted > 0 then visit_id end) as converted_traffic,
  count(distinct case when converted > 0 and platform in ('mobile_web','desktop') then visit_id end) as web_converted_traffic,
  count(distinct case when platform in ('mobile_web','desktop') and converted > 0 and event_type in ('shop_home')then visit_id end) as sh_converted_web_traffic
from etsy-data-warehouse-prod.weblog.visits v
left join etsy-data-warehouse-prod.weblog.events e using (visit_id)
where v._date >= current_date-30

----------------------------------------------------------------------
-- VISIT, GMS, CONVERSION COVERAGE OF VISITS THAT VIEWED SHOP HOME
----------------------------------------------------------------------
-- criteria: visits must have been the shop home page for their conversion and gms to contribute to that shop
-- create or replace table etsy-data-warehouse-dev.madelinecollins.web_shop_visits as (
-- select
--   platform,
--   beacon.event_name, 
--   (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id, 
--   (select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id, 
--   visit_id, 
--   sequence_number,
-- from
--   `etsy-visit-pipe-prod.canonical.visit_id_beacons`
-- inner join 
--   etsy-data-warehouse-prod.weblog.visits using (visit_id)
-- where
--   date(_partitiontime) >= current_date-30
--   and _date >= current_date-30
--   and platform in ('mobile_web','desktop','boe')
--   and (beacon.event_name in ('shop_home'))
-- group by all
-- );

with gms_and_conversion as ( -- get gms/ conversion from any visit that viewed shop home in last 30 days 
select
  g.seller_user_id,
  v.visit_id as converted_visit, -- if a visit made a purchase from that store, they converted 
  sum(gms_net) as gms_from_visit
from 
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits sv
inner join 
  etsy-data-warehouse-prod.transaction_mart.transactions_visits v using (visit_id) -- only looking at transaction data from visits that viewed shop
inner join
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans g
    on g.transaction_id=v.transaction_id 
where 
  g.date >= current_date-30 -- this will alsohave to be the last 30 days, since looking at a visit level 
group by all 
)
, shop_sections as (
select 
  shop_id,
  user_id as seller_user_id, 
  count(distinct name) as sections
from 
  etsy-data-warehouse-prod.etsy_shard.shop_sections
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics using (shop_id)
where
  active_listing_count > 0 
  and active_seller_status = 1
group by all
)
select
  case when s.seller_user_id is not null then 1 else 0 end as has_section,
  count(distinct v.visit_id) as total_visits,
  count(distinct gc.converted_visit) as converted_visits,
  sum(gms_from_visit) as total_gms
from 
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits v
left join 
  gms_and_conversion gc
    on v.seller_user_id= cast(gc.seller_user_id as string)
    and v.visit_id=gc.converted_visit
left join 
  shop_sections s
    on cast(s.seller_user_id as string) = v.seller_user_id 
group by all

----------------------------------------------------------------------
-- GMS FROM SHOP (including from traffic that did not see shop home) 
----------------------------------------------------------------------

----------------------------------------------------------------------
-- SECTION DISTRO ACROSS SHOPS 
----------------------------------------------------------------------
-- what % of visited shops have sections?
with shop_sections as (
select 
  shop_id,
  user_id as seller_user_id, 
  count(distinct name) as sections
from 
  etsy-data-warehouse-prod.etsy_shard.shop_sections
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics using (shop_id)
where
  active_listing_count > 0 
  and active_seller_status = 1
group by all
)
select
  case when s.seller_user_id is not null then 1 else 0 end as has_section,
  count(distinct v.seller_user_id) as visited_shops,
from 
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits v
left join 
  shop_sections s 
    on v.seller_user_id = cast(s.seller_user_id as string)
group by all

--active sellers w sections
-- what % of visited shops have sections?
with shop_sections as (
select 
  shop_id,
  user_id as seller_user_id, 
  count(distinct name) as sections
from 
  etsy-data-warehouse-prod.etsy_shard.shop_sections
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics using (shop_id)
where
  active_listing_count > 0 
  and active_seller_status = 1
group by all
)
select
  case when s.seller_user_id is not null then 1 else 0 end as has_section,
  count(distinct v.user_id) as visited_shops,
from 
  etsy-data-warehouse-prod.rollups.seller_basics v
left join 
  shop_sections s 
    on v.user_id = s.seller_user_id
where active_seller_status= 1
group by all

