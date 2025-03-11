----------------------------------------------------------------------
-- RUN QUERY TO GET SHOP HOME VISIT INFO 
----------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.shop_home_visits as (
select
  platform,
  beacon.event_name, 
  (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id, 
  (select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id, 
  visit_id, 
  sequence_number,
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
inner join 
  etsy-data-warehouse-prod.weblog.visits using (visit_id)
where
  date(_partitiontime) >= current_date-30
  and _date >= current_date-30
  and platform in ('mobile_web','desktop')
  and (beacon.event_name in ('shop_home'))
group by all
);

----------------------------------------------------------------------------------------------------
-- CREATE TABLE TO GET SECTIONS FOR ALL SHOPS (only looking at shops with sections) 
----------------------------------------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.section_names as (
with translated_sections as ( -- grab english translations, or whatever translation is set to 1
select 
  *
from etsy-data-warehouse-prod.etsy_shard.shop_sections_translations
qualify row_number() over (
    partition by id 
    order by
        case when language = 5 then 1 else 2 end,  -- Prioritize language = 5
        language asc  -- If no language = 5, take the lowest language number
) = 1
)
select 
  b.shop_id,
  b.user_id as seller_user_id,
  shop_name,
  coalesce(is_etsy_plus,0) as is_etsy_plus,
  seller_tier_new,
  case when (s.shop_id is not null or t.shop_id is not null) then 1 else 0 end as has_sections,
  coalesce(coalesce(nullif(s.name, ''),t.name), 'missing section name') as section_name,
  active_listing_count as active_listings ,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
inner join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id) -- only looking at shops with sections 
left join 
  translated_sections t 
    on s.shop_id=t.shop_id
    and s.id=t.id
where
  active_seller_status = 1 -- active sellers
  and is_frozen = 0  -- not frozen accounts 
  and active_listings > 0 -- shops with active listings
group by all
);
  
--------------------------------------------------
-- COMBINE SECTIONS TO GET GMS/ CR/ VISIT COVERAGE
--------------------------------------------------
with shop_sections as ( -- active shops + if they have sections with listings in them 
select 
  shop_id,
  seller_user_id,
  shop_name,
  sections_w_listings
from 
  etsy-data-warehouse-dev.madelinecollins.active_shops_and_section_info
group by all
)
, shop_visits as ( -- visits that viewed a shop on web
select
  seller_user_id,
  visit_id,
  count(sequence_number) as views
from  
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits v
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics b
    on v.seller_user_id=cast(b.user_id as string)
where 
  platform in ('desktop','mobile_web')
  ---here, i am only counting visits to active shops that are not frozen and have active listings
  and active_seller_status = 1 -- active sellers
  and is_frozen = 0  -- not frozen accounts 
  and active_listings > 0 -- shops with active listings
group by all 
)
, shop_gms_converts as ( -- get all shop info at the visit_id level
select
  g.seller_user_id,
  v.visit_id, 
  sum(gms_net) as gms_net,
from
  etsy-data-warehouse-prod.transaction_mart.transactions_visits v 
inner join
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans g
    on g.transaction_id=v.transaction_id 
where 
  g.date >= current_date-30 -- this will also have to be the last 30 days, since looking at a visit level 
group by all 
)
, shop_level as ( -- get everything to seller_user_id level 
select
  v.seller_user_id,
  v.visit_id,
  sum(v.views) as pageviews,
  case when gc.visit_id is not null then 1 else 0 end as converts, 
  sum(gms_net) as gms_net
from 
  shop_visits v
left join 
  shop_gms_converts gc 
    on v.seller_user_id= cast(gc.seller_user_id as string)
    and v.visit_id=gc.visit_id
group by all 
)
select
  case when coalesce(s.sections_w_listings,0) > 0 then 1 else 0 end as has_sections,
  count(distinct l.seller_user_id) as visited_shops,
  count(distinct visit_id) as visits,
  sum(pageviews) as pageviews,
  count(distinct case when converts > 0 then visit_id end) as converts,
  sum(gms_net) as gms_net
from 
  shop_level l -- starting with all visited shops, then seeing if those shops 
left join 
  shop_sections s
    on l.seller_user_id= cast(s.seller_user_id as string)
group by all 
order by 1 desc


--------------------------------------------------
-- share of sections without names 
--------------------------------------------------
with translated_sections as ( -- grab english translations, or whatever translation is set to 1
select 
  *
from etsy-data-warehouse-prod.etsy_shard.shop_sections_translations
qualify row_number() over (
    partition by id 
    order by
        case when language = 5 then 1 else 2 end,  -- Prioritize language = 5
        language asc  -- If no language = 5, take the lowest language number
) = 1
)
select 
  count(s.id) as names,
  sum(case when coalesce(nullif(s.name, ''),t.name) is not null then 1 else 0 end) as filled_in,
  sum(case when coalesce(nullif(s.name, ''),t.name) is null then 1 else 0 end) as missing_ids,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
inner join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
left join 
  translated_sections t 
    on s.shop_id=t.shop_id
    and s.id=t.id
where
  active_seller_status = 1 -- active sellers
  and is_frozen = 0  -- not frozen accounts 
  and active_listings > 0 -- shops with active listings
group by all
-- names	filled_in	missing_ids
-- 9585659	9568522	17137

--------------------------------------------------
-- TESTING
--------------------------------------------------
-- TEST 1: how many section names are getting repeated? is this because they actually show up twice? 
select shop_id, shop_name, section_name, count(*) from etsy-data-warehouse-dev.madelinecollins.section_names where section_name not in ('missing section name') and active_listings > 0 group by all order by 4 desc limit 5
-- shop_id	shop_name	section_name	f0_
-- 13042804	SoleilDuNordShop		5
-- 5437285	ahldraws	prints	3 --------> SHOWS UP THREE TIMES 
-- 10972477	Maryandpatch	Patterns in French	2--------> SHOWS UP TWICE  
-- 38539730	HelloEloCha	Spring	2--------> SHOWS UP TWICE  
-- 33053189	ChromaticsDesigns	Art	2--------> SHOWS UP TWICE  

with agg as (
select 
  shop_id, 
  shop_name, 
  section_name, 
  count(*) as total_count
from etsy-data-warehouse-dev.madelinecollins.section_names 
where 
  section_name not in ('missing section name')
  and active_listings > 0 
group by all 
order by 4 desc 
)
select 
  total_count,
  count(section_name) as names
from agg 
group by all
order by 1 asc
-- total_count	names
-- 1	6498023
-- 2	112
-- 3	1
-- 5	1
