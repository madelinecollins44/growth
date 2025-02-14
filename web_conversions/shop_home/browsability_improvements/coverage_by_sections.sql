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
-- OF SHOPS W SECTIONS, HOW MANY DO THEY HAVE
----------------------------------------------------------------------
  with shop_sections as ( -- active shops + if they have sections with listings in them 
select 
  b.shop_id,
  count(distinct case when active_listing_count > 0 then s.name end) as sections
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
where
  active_seller_status = 1
group by all
)
select
  sections,
  count(distinct shop_id) as shops
from shop_sections
group by all 
  
----------------------------------------------------------------------
-- VISIT, GMS, CONVERSION COVERAGE OF VISITS THAT VIEWED SHOP HOME
----------------------------------------------------------------------
---- What do CR & GMS coverage & visits look like for shops with sections vs shops without? 
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

with shop_sections as ( -- active shops + if they have sections with listings in them 
select 
  b.shop_id,
  shop_name,
  b.user_id as seller_user_id, 
  count(distinct case when active_listing_count > 0 then s.name end) as sections
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
where
  active_seller_status = 1
group by all
)
, shop_visits as ( -- visits that viewed a shop on web
select
  seller_user_id,
  visit_id,
  count(sequence_number) as views
from  
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits 
where 
  platform in ('desktop','mobile_web')
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
  count(distinct v.visit_id) as visits,
  count(distinct gc.visit_id) as converts, 
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
  case when s.sections > 0 then 1 else 0 end as has_sections,
  count(distinct l.seller_user_id) as visited_shops,
  sum(visits) as visits,
  sum(converts) as converts,
  sum(gms_net) as gms_net
from 
  shop_sections s -- starting here to get all active shops, and then looking at whether or not those were visited. some shops that were visited are not active.
left join 
  shop_level l
    on l.seller_user_id= cast(s.seller_user_id as string)
group by all 


----------------------------------------------------------------------
-- GMS FROM SHOP (including from traffic that did not see shop home) 
----------------------------------------------------------------------

----------------------------------------------------------------------
-- SECTION DISTRO ACROSS SHOPS 
----------------------------------------------------------------------
-- visited shops by section
 with shop_sections as (
select 
  shop_id,
  user_id as seller_user_id, 
  count(distinct name) as sections
from 
  etsy-data-warehouse-prod.rollups.seller_basics 
left join
  (select * from etsy-data-warehouse-prod.etsy_shard.shop_sections where active_listing_count > 0 ) using (shop_id)
where
 active_seller_status = 1
group by all
)
select
  case when sections > 0 then 1 else 0 end as has_section,
  count(distinct v.seller_user_id) as visited_shops,
from 
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits v
inner join 
  shop_sections s -- this includes all active shops
    on v.seller_user_id = cast(s.seller_user_id as string)
where platform in ('mobile_web','desktop')
group by all
  
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
where platform in ('mobile_web','desktop')
group by all

--active sellers w sections
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

-------------------------------------------------------
-- TESTING
-------------------------------------------------------

/* TEST 1: see how accurate beacons count are vs events*/ 
select visit_id, count(visit_id) from etsy-data-warehouse-dev.madelinecollins.web_shop_visits
group by all order by 2 desc limit 5
-- -- visit_id	f0_
-- 1jFttPKXT0fRycXZM4FWopJ-bBsz.1739258732525.14	888
-- 1jFttPKXT0fRycXZM4FWopJ-bBsz.1739255034370.12	856
-- HAZGJf8xWWeiEUqd_JXqx9fpDHGI.1739188263736.11	850
-- Cq3mhqPxGRxIT2fsjw9Ny_P-spBa.1738907747378.10	846
-- xezpADXyll34GnosPXfga5xHrWlK.1739356236566.14	845
select visit_id, count(sequence_number)
from etsy-data-warehouse-prod.weblog.events
where event_type in ('shop_home')
and visit_id in ('1jFttPKXT0fRycXZM4FWopJ-bBsz.1739258732525.14','1jFttPKXT0fRycXZM4FWopJ-bBsz.1739255034370.12','HAZGJf8xWWeiEUqd_JXqx9fpDHGI.1739188263736.11','Cq3mhqPxGRxIT2fsjw9Ny_P-spBa.1738907747378.10')
group by all

/* TEST 2: how many visits went to a shop with a section, shop without a section, or both?*/ 
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
, visit_status as (
select
  v.visit_id,
  coalesce(max(case when s.shop_id IS NOT NULL THEN 1 ELSE 0 END), 0) AS has_section,
  coalesce(min(case when  s.shop_id IS NULL THEN 1 ELSE 0 END), 0) AS no_section
from 
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits v
left join 
  shop_sections s 
    on v.seller_user_id = cast(s.seller_user_id as string)
  group by all 
)
SELECT 
    CASE 
        WHEN has_section = 1 AND no_section = 0 THEN 'Only Shops with Sections'
        WHEN has_section = 0 AND no_section = 1 THEN 'Only Shops without Sections'
        WHEN has_section = 1 AND no_section = 1 THEN 'Both Types of Shops'
    END AS visit_category,
    COUNT(DISTINCT visit_id) AS visit_count
FROM visit_status
GROUP BY all;



/* TEST 3: check on shop level (looking at most visited shops/ most converts) */ 
-- shop_name	        shop_id	    seller_user_id	    has_sections	visited_shops	visits	converts	gms_net
-- Beniouraincarpets	7746216	      16263321	          1	      z1	263747	29	18647.07692312
-- jmdceramicsart	18990914	183816016	1	1	238216	2	217.69230769
-- BeadLetsbyNatalie	32078771	539190449	1	1	183179	0	
-- CYOstore	25571082	371259652	1	1	168237	3	318
-- XtraDark	57192578	1041209412	0	1	166053	2	132.92307692

-- check visits

-- check gms/ converts 
select
  g.seller_user_id,
  count(distinct v.visit_id) as converted_visits, -- if a visit made a purchase from that store, they converted 
  count(distinct case when sv.visit_id is not null then v.visit_id end) as converted_shop_visits,
  sum(gms_net) as gms_net,
  sum(distinct case when sv.visit_id is not null then gms_net end) as converted_shop_gms
from
  etsy-data-warehouse-prod.transaction_mart.transactions_visits v 
inner join
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans g -- only looking at gms from visits that viewed  
    on g.transaction_id=v.transaction_id 
left join 
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits sv
    on sv.visit_id=v.visit_id
    and sv.seller_user_id=cast(g.seller_user_id as string)
where 
  g.date >= current_date-30 -- this will also have to be the last 30 days, since looking at a visit level 
  and g.seller_user_id in (16263321,183816016,539190449,371259652,1041209412)
group by all 
-- seller_user_id	converted_visits	converted_shop_visits	gms_net	converted_shop_gms
-- 1041209412	2	2	398.76923076	66.46153846
-- 183816016	2	2	217.69230769	217.69230769
-- 16263321	29	29	44663.23076931	17672.92307696
-- 371259652	3	3	1936.15384616	318
