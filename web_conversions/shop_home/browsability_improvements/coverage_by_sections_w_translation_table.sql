--------------------------------------------------
-- OVERALL COUNTS TO CONFIRM
--------------------------------------------------
--overall visit to active, nonfrozen shops with active listings
with shop_visits as ( -- visits that viewed a shop on web
select
  shop_id,
  seller_user_id,
  visit_id,
  count(sequence_number) as views
from  
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits 
where 
  platform in ('desktop','mobile_web')
group by all 
)
select 
  count(distinct seller_user_id) as shops,
  count(distinct visit_id) as visits,
  sum(views) as total_pageviews,
from shop_visits v
inner join etsy-data-warehouse-prod.rollups.seller_basics  b
  on v.seller_user_id=cast(b.user_id as string)
where 
  active_seller_status = 1 -- active sellers
  and is_frozen = 0  -- not frozen accounts 
  and active_listings > 0 -- shops with active listings
  -- and b.shop_id in (46026156)

--overall gms from last 30 days (all platforms, just web)
select
  sum(gms_net) as gms_from_visit
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans g
inner join 
  etsy-data-warehouse-prod.transaction_mart.transactions_visits using (transaction_id)
where 
  g.date >= current_date-30 
  and platform_app in ('mobile_web','desktop')
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
-- RUN QUERY TO GET SHOP HOME VISIT INFO 
----------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.web_shop_visits as (
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
  and platform in ('mobile_web','desktop','boe')
  and (beacon.event_name in ('shop_home'))
group by all
);

--------------------------------------------------
-- CREATE TABLE TO GET SECTIONS FOR ALL SHOPS
--------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.active_shops_and_section_info as (
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
  -- case when (s.shop_id is not null or t.shop_id is not null) and active_listing_count > 0 then 1 else 0 end as has_sections_w_listings,
  case when (s.shop_id is not null or t.shop_id is not null) then 1 else 0 end as has_sections,
  count(s.id) as sections,
  count(case when active_listing_count > 0 then s.id end) as sections_w_listings,
  count(case when ((coalesce(nullif(s.name, ''),t.name)) is not null) and active_listing_count > 0 then s.id end) as filled_ids,
  count(case when ((coalesce(nullif(s.name, ''),t.name)) is null) and active_listing_count > 0 then s.id end) as missing_ids,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
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
-- HOW MANY LISTINGS ARE IN EACH SECTION?
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
, active_shops_section as (
select 
  b.shop_id,
  b.shop_name,
  coalesce(s.id,0) as section,-- if null, no sections
  coalesce(nullif(s.name, ''),t.name) as section_name,
  sum(active_listing_count) as active_listing_count,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
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
)
select
  -- count(distinct v.shop_id) as shops,
  -- s.shop_name,
  case 
    when coalesce(active_listing_count,0) = 0 then '0'
    when coalesce(active_listing_count,0) = 1 then '1'
    when coalesce(active_listing_count,0) > 1 and coalesce(active_listing_count,0) <=5 then '2-5'
    when coalesce(active_listing_count,0) > 5 and coalesce(active_listing_count,0) <=10 then '6-10'
    when coalesce(active_listing_count,0) > 10 and coalesce(active_listing_count,0) <=20 then '11-20'
    when coalesce(active_listing_count,0) > 20 and coalesce(active_listing_count,0) <=50 then '21-50'
    when coalesce(active_listing_count,0) > 50 and coalesce(active_listing_count,0) <=75 then '51-75'
    when coalesce(active_listing_count,0) > 75 and coalesce(active_listing_count,0) <=100 then '76-100'
    else '100+'
    -- when coalesce(active_listing_count,0) > 100 and coalesce(active_listing_count,0) <=5 then '1-5'
  end as active_listing_count,
  coalesce(count(distinct section),0) as sections_w_listings_active_shops, -- how many sections have this many active listings
  coalesce(count(distinct case when v.shop_id is not null then s.section end),0) as sections_w_listings_viisted_shops
from 
  active_shops_section s
left join
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits v
    on cast(s.shop_id as string) =v.shop_id
group by all 
order by 1 asc 

----------------------------------------------------------------------------------------------------
-- WHAT % OF SHOP HOME TRAFFIC TO SHOPS WITH 100+ LISTINGS HAVE SECTIONS WITH 21-50 LISTINGS IN A SECTION?
----------------------------------------------------------------------------------------------------
-- how many shop home ppageviews do shops with 100+ listings get?
with shop_visits as (
select
  shop_id,
  count(distinct visit_id) as unique_visits,
  count(sequence_number) as pageviews
from 
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits 
group by all 
)
, shops_w_100_listings as ( 
select distinct
  shop_id,
  shop_name,
  active_listings
from   
  etsy-data-warehouse-prod.rollups.seller_basics
where
  active_seller_status = 1 -- active sellers
  and is_frozen = 0  -- not frozen accounts 
  and active_listings >= 100 -- shops w/ 100 + listings
order by active_listings asc
)
select
  avg(active_listings) as avg_listing_count,
  sum(pageviews) as pageviews
from shop_visits v
inner join shops_w_100_listings s
on cast(s.shop_id as string) =v.shop_id

-- total traffic to shop home pages for shops with 100+ listings AND sections with 21-50 listings
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
, active_shops_section as ( -- sections x listing in each section by shop 
select 
  b.shop_id,
  b.shop_name,
  s.id as section,
  coalesce(nullif(s.name, ''),t.name) as section_name,
  sum(active_listing_count) as active_listing_count,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
left join 
  translated_sections t 
    on s.shop_id=t.shop_id
    and s.id=t.id
where
  active_seller_status = 1 -- active sellers
  and is_frozen = 0  -- not frozen accounts 
  and active_listings >= 100 -- shops w/ 100 + listings
group by all
)
, listings_per_section as ( -- only grab shops that have some sections with 21-50 listings
select
  shop_id,
  coalesce(count(distinct section_name),0) as number_of_sections,
  avg(active_listing_count) as avg_listings_per_section,
from 
  active_shops_section
where 
  coalesce(active_listing_count,0) > 20 and coalesce(active_listing_count,0) <=50  -- only counting sections with between 21-50 listings
group by all
)
, shop_visits as ( -- agg all shop home visits 
select
  shop_id,
  count(distinct visit_id) as unique_visits,
  count(sequence_number) as pageviews
from 
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits 
group by all 
)
select -- only look at 
  number_of_sections,
  count(distinct v.shop_id) as shops,
  sum(pageviews) as pageviews
  -- avg(number_of_sections) as avg_number_of_sections,
  -- avg(avg_listings_per_section) as avg_listings_per_section,  
from 
  listings_per_section s
inner join
  shop_visits v
    on cast(s.shop_id as string) =v.shop_id
group by all 

----------------------------------------------------------------------------------------------------
-- OF SHOPS WITH X SECTIONS, HOW MANY LISTINGS DO THEY TYPICALLY HAVE IN THAT SECTION
----------------------------------------------------------------------------------------------------
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
, active_shops_section as (
select 
  b.shop_id,
  b.shop_name,
  s.id as section,
  active_listings,
  coalesce(nullif(s.name, ''),t.name) as section_name,
  active_listing_count,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
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
)
, shop_visits as ( -- agg all shop home visits 
select
  shop_id,
  count(distinct visit_id) as unique_visits,
  count(sequence_number) as pageviews
from 
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits 
group by all 
)
, shop_level as (
select
  v.shop_id,
  s.shop_name,
  coalesce(active_listings,0) as active_listings,
  coalesce(count(distinct section),0) as sections_w_listings, -- how many sections have this many active listings
  coalesce(avg(active_listing_count),0) as avg_listings_per_sections
from 
  active_shops_section s
inner join
  shop_visits v
    on cast(s.shop_id as string) =v.shop_id
where 
  active_listing_count > 0 -- only count sections with active listings 
group by all 
)
select
  -- shop_id,
  -- shop_name,
  sections_w_listings, 
  avg(avg_listings_per_sections) as avg_listings_per_sections, -- across all shops
  avg(active_listings) as active_listings_in_each_shop
from shop_level
group by all  
order by 1 asc
  
--------------------------------------------------
--TESTING
--------------------------------------------------
-- TEST 1: check to make sure joining to tables make sense 
select 
--s.*,t.*
  b.shop_id,
  b.seller_tier_new,
  shop_name,
  b.user_id as seller_user_id, 
  -- coalesce(nullif(s.name, ''),t.name) ,
  case when active_listing_count > 0 then coalesce(nullif(s.name, ''),t.name) end as sections,
  count(distinct case when active_listing_count > 0 then coalesce(nullif(s.name, ''),t.name) end) as sections
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections_translations t 
    on s.shop_id=t.shop_id
    and s.id=t.id
    and s.create_date=t.create_date
where
  active_seller_status = 1
  and s.shop_id in (46345829)
group by all

-- TEST 2: make sure has_section count is accurate
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
, section_count as (
select 
  b.shop_id,
  shop_name,
  seller_tier_new,
  case when (s.shop_id is not null or t.shop_id is not null) and active_listing_count > 0 then 1 else 0 end as has_sections,
  count(case when active_listing_count > 0 then s.id end) as sections,
  count(case when ((coalesce(nullif(s.name, ''),t.name)) is not null) and active_listing_count > 0 then s.id end) as filled_ids,
  count(case when ((coalesce(nullif(s.name, ''),t.name)) is null) and active_listing_count > 0 then s.id end) as missing_ids,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
left join 
  translated_sections t 
    on s.shop_id=t.shop_id
    and s.id=t.id
where
  active_seller_status = 1 -- active sellers
  and is_frozen = 0  -- not frozen accounts 
  and active_listings > 0 -- shops with active listings
  -- and b.shop_id in (20077844)
group by all
)
select
*
from section_count
where has_sections = 1 and sections = 0 

-- TEST 2: make sure totals of each cte match up 
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
/* select count(distinct shop_id) as shops, count(distinct case when sections_w_listings > 0 then shop_id end) as shops_w_sections from shop_sections
--shops: 2935061
--shops w sections: 1371999

select seller_user_id, count(*) from shop_sections group by all order by 2 desc limit 5
--each seller_user_id is unique */

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
/* select count(distinct visit_id) as visits, sum(views), count(distinct seller_user_id) as shops  from shop_visits 
--visits:108650359
--pageviews:214520134
--shops: 2614974

select seller_user_id, visit_id, count(*) from shop_visits group by all order by 3 desc limit 5
--each seller_user_id, visit_id is unique 
*/

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
/* select count(distinct seller_user_id) as shops, count(distinct visit_id) as visits, sum(gms_net) as gms from shop_gms_converts
--visits:
--pageviews:

select seller_user_id, visit_id, count(*) from shop_gms_converts group by all order by 3 desc limit 5
--each seller_user_id, visit_id is unique 
*/
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
/* select count(distinct visit_id) as visits, sum(pageviews) as pageviews, count(distinct case when converts > 0 then visit_id end) as converted_visits, sum(gms_net) from shop_level 
--visits: 108650359
--pageviews: 214520134
--converted visits: 2471995
--gms_net: 108039045.9956857

select seller_user_id, visit_id, count(*) from shop_level group by all order by 3 desc limit 5
--each seller_user_id, visit_id is unique */

select
  -- case when coalesce(s.sections_w_listings,0) > 0 then 1 else 0 end as has_sections,
  count(distinct l.seller_user_id) as visited_shops,
  count(distinct visit_id) as visits,
  sum(pageviews) as pageviews,
  count(distinct case when converts > 0 then visit_id end) as converts,
  sum(gms_net) as gms_net
from 
  shop_level  l -- starting with all visited shops
left join 
  shop_sections s
    on l.seller_user_id= cast(s.seller_user_id as string)
group by all 
order by 1 desc
/* select count(distinct visit_id) as visits, sum(pageviews) as pageviews, count(distinct case when converts > 0 then visit_id end) as converted_visits, sum(gms_net) from shop_level 
--visits: 108650359
--pageviews: 214520134
--converted visits: 2471995
--gms_net: 108039045.9956857
--shops:2614974

*/


-- TEST 3: how many visits view multiple shops?
with shop_visits as ( -- visits that viewed a shop on web
select
  visit_id,
  count(distinct seller_user_id) as shops,
  count(sequence_number) as views
from  
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits 
where 
  platform in ('desktop','mobile_web')
group by all 
)
select
  case when shops > 1 then 1 else 0 end as more_than_1_shop,
  count(distinct visit_id) as visits
from shop_visits
group by all 
-- more_than_1_shop	visits --> 10% of visits only look at one shop
-- 0	125225198
-- 1	14351093

/* how many visits are getting double counted? */
with shop_visits as ( -- visits that viewed a shop on web
select
  visit_id,
  seller_user_id,
  count(sequence_number) as views
from  
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits 
where 
  platform in ('desktop','mobile_web')
group by all 
)
, shop_sections as ( -- active shops + if they have sections with listings in them 
select 
  case when sections_w_listings > 0 then 1 else 0 end as has_sections,
  seller_user_id,
from 
  etsy-data-warehouse-dev.madelinecollins.active_shops_and_section_info
group by all
)
, visits_overlap as (
select 
  visit_id,
  count(distinct has_sections) as types_of_shops,
  count(distinct v.seller_user_id) as shops,
from shop_visits v
left join shop_sections s 
    on v.seller_user_id= cast(s.seller_user_id as string)
group by all 
order by 2 desc
)
select 
  types_of_shops,
  count(distinct visit_id) as visits
from visits_overlap
group by all 
-- types_of_shops	visits
-- 0	30925932
-- 1	104316226
-- 2	4334133


-- TEST 4: what % of shop home traffic is to etsy plus sellers?
with shop_visits as ( -- visits that viewed a shop on web
select
  seller_user_id,
  is_etsy_plus,
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
select
  is_etsy_plus,
  count(distinct seller_user_id) as shops,
  count(distinct visit_id) as visits,
  sum(views) as pageviews
from shop_visits
group by all 

-- TEST 5: how many unique sections are there (distro of listings in each section)
select 
  count(distinct case when b.active_listings > 0 then s.id end) as shops_w_0_plus_listings_sections,
  count(distinct case when b.active_listings > 10 then s.id end) as shops_w_10_plus_listings_sections,
  count(distinct case when b.active_listings > 50 then s.id end) as shops_w_50_plus_listings_sections,
  count(distinct case when b.active_listings > 100 then s.id end) as shops_w_100_plus_listings_sections,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
-- left join 
--   translated_sections t 
--     on s.shop_id=t.shop_id
--     and s.id=t.id
where
  b.active_seller_status = 1 -- active sellers
  and b.is_frozen = 0  -- not frozen accounts 
  -- and b.active_listings > 0 -- shops with active listings
  and s.id is not null 
group by all
-- shops_w_0_plus_listings_sections	shops_w_10_plus_listings_sections	shops_w_50_plus_listings_sections	shops_w_100_plus_listings_sections
-- 9608445	7615452	4574655	3024597
