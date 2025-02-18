--"What % of users on web view multiple listings from the same shop from Shop home? 
-- Use case - As a buyer viewing multiple listings from the same shop, I am able to easily access my most recently viewed listings from that shop."

---------------------------------------------------------------------------------------------------------------------------------------
-- overall page traffic over last 30 days by platform
---------------------------------------------------------------------------------------------------------------------------------------
-- overall traffic over last 30 days (visits)
select
  platform,  
  count(distinct visit_id) as traffic,
  sum(total_gms) as total_gms
from 
  etsy-data-warehouse-prod.weblog.visits
where 
  _date >= current_date-30 
  and platform in ('boe','desktop','mobile_web')
group by all 
  
-- overall page traffic over last 30 days (visits)
with shop_home_visits as (
select distinct 
  visit_id
from 
  etsy-data-warehouse-prod.weblog.events e
where 
  _date >= current_date-30 
  and e.event_type in ('shop_home')
)
select
  platform,
  count(distinct visit_id) as traffic,
  sum(total_gms) as total_gms
from 
  shop_home_visits shv
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where v._date >= current_date-30
group by all
order by 1 asc

-- listing views from shop home referrer on web over last 30 days (visits)
select 
  count(distinct visit_id) 
from etsy-data-warehouse-prod.analytics.listing_views 
where _date >= current_date-30 
    and platform in ('mobile_web','desktop') 
    and referring_page_event in ('shop_home')

-- overall page traffic over last 30 days (users)
select
  platform,
  count(distinct mapped_user_id) as users,
  count(distinct v.visit_id) as visits,
  count(distinct case when v.user_id is not null then v.visit_id end) as signed_in_visits,
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e
    on v.visit_id=e.visit_id
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile p
    on v.user_id = p.user_id
where 
  platform in ('mobile_web','desktop','boe')
  and v._date >= current_date-30
  and event_type in ('shop_home')
group by all

-- listing views from shop home referrer on web over last 30 days (users)
with mapped_users as (
select
  visit_id,
  mapped_user_id
from 
  etsy-data-warehouse-prod.weblog.visits
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile using (user_id)
where 
  platform in ('mobile_web','desktop','boe')
  and _date >= current_date-30
)
select 
  count(distinct mapped_user_id) as total_users, 
  count(distinct visit_id) as total_visits,
  count(distinct case when mapped_user_id is not null then visit_id end) as signed_in_visits,
from etsy-data-warehouse-prod.analytics.listing_views 
inner join mapped_users using (visit_id)
where 
  _date >= current_date-30 
  and referring_page_event in ('shop_home')

---------------------------------------------------------------------------------------------------------------------------------------
-- all visits to view multiple listings from the same shop
---- does not exclude sellers or self-visits
---------------------------------------------------------------------------------------------------------------------------------------
with active_listings as ( -- need this to pull in shop_id
select
  listing_id,
  shop_id
from etsy-data-warehouse-prod.rollups.active_listing_basics
)
, shop_home_listing_views as ( -- start with pulling all data on listing views from shop_home page. 
select
  -- lv.platform,
  visit_id,
  shop_id,
  listing_id,
  count(visit_id) as listing_views,
  sum(purchased_after_view) as purchased_after_view,
from 
  etsy-data-warehouse-prod.analytics.listing_views
inner join  
  active_listings using (listing_id)
where 
  _date >= current_date-30 
  and platform in ('mobile_web','desktop') 
  and referring_page_event in ('shop_home')
group by all 
)
, gms as (-- then pull all the gms for visits/ listings
select
  a.listing_id, 
  v.visit_id,
  sum(gms_net) as gms_net
from 
  etsy-data-warehouse-prod.transaction_mart.all_transactions a
inner join 
  etsy-data-warehouse-prod.transaction_mart.transactions_visits v using (transaction_id)
left join 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans g 
    on a.transaction_id=g.transaction_id
group by all 
)
, views_and_gms as (
select 
  lv.visit_id,
  lv.shop_id,
  count(distinct lv.listing_id) as unique_listings_viewed,
  count(lv.visit_id) as listing_views,
  sum(lv.purchased_after_view) as purchased_after_view,
  sum(gms_net) as gms_net
from 
  shop_home_listing_views lv 
left join 
  gms using (visit_id, listing_id)
group by all 
)
select  
  -- platform,
  case -- unique listings viewed from a single seller
    when unique_listings_viewed = 1 then '1'
    when unique_listings_viewed = 2 then '2'
    when unique_listings_viewed = 3 then '3'
    when unique_listings_viewed = 4 then '4'
    when unique_listings_viewed = 5 then '5'
    when unique_listings_viewed between 6 and 10 then '6-10'
    when unique_listings_viewed between 11 and 20 then '11-20'
    when unique_listings_viewed between 21 and 30 then '21-30'
    when unique_listings_viewed between 31 and 40 then '31-40'
    when unique_listings_viewed between 41 and 50 then '41-50'
    else '50+'
  end as unique_listings_viewed,
  count(distinct visit_id) as visits_view_listings_from_shop_home,
  sum(listing_views) as shop_home_listing_views,
  sum(gms_net) as gms_net,
from 
  views_and_gms
group by all 
order by 1 asc


---------------------------------------------------------------------------------------------------------------------------------------
-- all visits to view multiple listings from the same shop EXCLUDING sellers
---------------------------------------------------------------------------------------------------------------------------------------
with active_listings as ( -- need this to pull in shop_id
select
  listing_id,
  shop_id
from etsy-data-warehouse-prod.rollups.active_listing_basics
)
, seller_status as (
select
  platform,
  visit_id,
  case when s.user_id is not null and active_seller_status = 1 then 1 else 0 end as is_active_seller,
from 
  etsy-data-warehouse-prod.weblog.visits v
left join  
  etsy-data-warehouse-prod.rollups.seller_basics s 
    using (user_id)
where 
  platform in ('mobile_web','desktop','boe')
  and _date >= current_date-30
)
, shop_home_listing_views as ( -- start with pulling all data on listing views from shop_home page. this is all at the shop_id level. 
select
  s.platform,
  lv.visit_id,
  is_active_seller,
  shop_id,
  count(distinct listing_id) as unique_listings_viewed,
  count(lv.visit_id) as listing_views,
  sum(purchased_after_view) as purchased_after_view,
from 
  etsy-data-warehouse-prod.analytics.listing_views lv
inner join  
  active_listings a using (listing_id)
inner join 
  seller_status s
    on s.visit_id=lv.visit_id
where 
  _date >= current_date-30 
  and referring_page_event in ('shop_home')
group by all 
)
select  
  -- platform,
  is_active_seller,
  case -- unique listings viewed from a single seller
    when unique_listings_viewed = 1 then '1'
    when unique_listings_viewed = 2 then '2'
    when unique_listings_viewed = 3 then '3'
    when unique_listings_viewed = 4 then '4'
    when unique_listings_viewed = 5 then '5'
    when unique_listings_viewed between 6 and 10 then '6-10'
    when unique_listings_viewed between 11 and 20 then '11-20'
    when unique_listings_viewed between 21 and 30 then '21-30'
    when unique_listings_viewed between 31 and 40 then '31-40'
    when unique_listings_viewed between 41 and 50 then '41-50'
    else '50+'
  end as unique_listings_viewed,
  count(distinct visit_id) as visits_view_listings_from_shop_home,
  sum(listing_views) as shop_home_listing_views,
from 
  shop_home_listing_views
group by all 
order by 1 asc

---------------------------------------------------------------------------------------------------------------------------------------
-- users to look at listings from same shop across visits
---------------------------------------------------------------------------------------------------------------------------------------
with active_listings as ( -- need this to pull in shop_id
select
  listing_id,
  shop_id
from etsy-data-warehouse-prod.rollups.active_listing_basics
)
, mapped_users as (
select
  visit_id,
  mapped_user_id
from 
  etsy-data-warehouse-prod.weblog.visits
inner join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile using (user_id)
where 
  platform in ('mobile_web','desktop','boe')
  and _date >= current_date-30
)
, shop_home_listing_views as ( -- start with pulling all data on listing views from shop_home page. this is all at the shop_id level. 
select
  -- lv.platform,
  mapped_user_id,
  shop_id,
  count(distinct lv.listing_id) as unique_listings_viewed,
  count(lv.visit_id) as listing_views,
  sum(purchased_after_view) as purchased_after_view,
from 
  mapped_users mu
inner join 
  etsy-data-warehouse-prod.analytics.listing_views lv using (visit_id)
inner join  
  active_listings al
    on lv.listing_id= al.listing_id
where 
  _date >= current_date-30 
  and platform in ('mobile_web','desktop') 
  and referring_page_event in ('shop_home')
group by all 
)
select  
  -- platform,
  case -- unique listings viewed from a single seller
    when unique_listings_viewed = 1 then '1'
    when unique_listings_viewed = 2 then '2'
    when unique_listings_viewed = 3 then '3'
    when unique_listings_viewed = 4 then '4'
    when unique_listings_viewed = 5 then '5'
    when unique_listings_viewed between 6 and 10 then '6-10'
    when unique_listings_viewed between 11 and 20 then '11-20'
    when unique_listings_viewed between 21 and 30 then '21-30'
    when unique_listings_viewed between 31 and 40 then '31-40'
    when unique_listings_viewed between 41 and 50 then '41-50'
    else '50+'
  end as unique_listings_viewed,
  count(distinct mapped_user_id) as users_view_listings_from_shop_home,
  -- count(distinct visit) as users_view_listings_from_shop_home,
  sum(listing_views) as shop_home_listing_views,
from 
  shop_home_listing_views
group by all 
order by 1 asc

----------------------------------------
--TESTING
----------------------------------------
-- TEST 1: make sure adding in seller_status doesnt mess up counts
with active_listings as ( -- need this to pull in shop_id
select
  listing_id,
  shop_id
from etsy-data-warehouse-prod.rollups.active_listing_basics
)
, seller_status as (
select
  platform,
  visit_id,
  case when s.user_id is not null and active_seller_status = 1 then 1 else 0 end as is_active_seller,
from 
  etsy-data-warehouse-prod.weblog.visits v
left join  
  etsy-data-warehouse-prod.rollups.seller_basics s 
    using (user_id)
where 
  platform in ('mobile_web','desktop')
  and _date >= current_date-30
)
-- , shop_home_listing_views as ( -- start with pulling all data on listing views from shop_home page. this is all at the shop_id level. 
select
  -- s.platform,
  count(distinct lv.visit_id),
  -- is_active_seller,
  -- shop_id,
  -- count(distinct listing_id) as unique_listings_viewed,
  -- count(lv.visit_id) as listing_views,
  -- sum(purchased_after_view) as purchased_after_view,
from 
  etsy-data-warehouse-prod.analytics.listing_views lv
inner join  
  active_listings a using (listing_id)
inner join 
  seller_status s
    on s.visit_id=lv.visit_id
where 
  _date >= current_date-30 
  and referring_page_event in ('shop_home')
  -- and lv.platform in ('mobile_web','desktop')
group by all 
-- 59359600 w sellers, exclude boe 34885893
-- 59359600, exclude boe 34885893



-- TEST 2: make sure math is working our on visit level
with active_listings as ( -- need this to pull in shop_id
select
  listing_id,
  shop_id
from etsy-data-warehouse-prod.rollups.active_listing_basics
)
select
  -- lv.platform,
  visit_id,
  shop_id,
  listing_id,
  count(visit_id) as listing_views,
  sum(purchased_after_view) as purchased_after_view,
from 
  etsy-data-warehouse-prod.analytics.listing_views
inner join  
  active_listings using (listing_id)
where 
  _date >= current_date-30 
  and platform in ('mobile_web','desktop') 
  and referring_page_event in ('shop_home')
group by all 
order by 4, 5 desc 
limit 5
-- visit_id	shop_id	listing_id	listing_views	purchased_after_view
-- 5beb5iltGYQost8UXfvaA2VaVT6U.1738546277696.1	57364273	1865406667	286	0
-- GwHHEB-WUvssuiIv3vp97lw1bmBS.1738633016379.1	57364273	1865988199	278	0
-- h1Nqd842i5ZQvo9LIi6T1SzGpE9-.1739347262802.1	57137102	1872200581	201	0
-- crx4-uaQNBlQo5lEkS40YQ-pQowe.1739348146625.1	57137102	1856709750	173	0
-- ASCk0mcfOqGiHOSs9-SRhQcpKRoY.1739348484280.1	57137102	1867434087	173	0
-- 7Yh8Tb2-uM-TZN-33rVD4zNP_QeT.1738538970257.1	15351076	824484505	1	1
-- elJYNzJ4TSdI2VhaQPUag8OG6Vbh.1738849753765.1	11751361	802391400	1	1
-- 3bD0kNYOp0LNopZ_SrPGFQ5FV-GS.1738672227608.1	5842826	1778410788	1	1
-- cyBpb8dLqZzIBaDRc-rk8Q4qpKC7.1738501230855.1	31500747	1302027560	1	1
-- MLn8L8Y4oKrKd9FvGNJ-San99t0V.1738946293237.1	54022536	1803628337	1	1

select
  a.listing_id, 
  v.visit_id,
  sum(gms_net) as gms_net
from 
  etsy-data-warehouse-prod.transaction_mart.all_transactions a
inner join 
  etsy-data-warehouse-prod.transaction_mart.transactions_visits v using (transaction_id)
left join 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans g 
    on a.transaction_id=g.transaction_id
where visit_id in ('5beb5iltGYQost8UXfvaA2VaVT6U.1738546277696.1')
and listing_id = 1865406667
group by all 
-- listing_id	visit_id	gms_net
-- 1803628337	MLn8L8Y4oKrKd9FvGNJ-San99t0V.1738946293237.1	19.90773077


-- TEST 3: look at specific visit that viewed many different shops 

select
  platform,
  visit_id,
  shop_id,
    -- count(distinct shop_id) as shops,
  count(distinct listing_id) as unique_listings,
  count(visit_id) as listing_views,
  sum(purchased_after_view) as purchased_after_view,
from 
  etsy-data-warehouse-prod.analytics.listing_views
inner join  
  active_listings using (listing_id)
where 
  _date >= current_date-30 
  and platform in ('mobile_web','desktop') 
  and referring_page_event in ('shop_home')
group by all 
order by 4, 6 desc
limit 5
-- platform	visit_id	shop_id	unique_listings	listing_views	purchased_after_view
-- desktop	yibFU0VsACwU7_xDklpmPnAgtWDB.1737497444301.2	12955584	1	55	55
-- desktop	NuF3AG-shvsVUgSoNnThXdVjDjKx.1738464333742.2	16922066	1	44	44
-- desktop	a7Ymw6Tv--QIBbV-7vElJxnShZs2.1739278104660.1	7948934	46	47	47
-- desktop	1SbOt4yYrTqWgz0SMpxKEfbzm_wO.1739178181600.2	22363515	38	39	39
-- desktop	N3ovfNfbxs7YvMJAUF7C_5ZFdvAT.1738338071784.1	10834560	36	38	38
-- desktop	r9vJqrLLDPUvRmk3X5JLWHslA7DB.1739448938182.5	16186538	36	36	36
-- desktop	nrm9LjD_FqDVgLMR-nDaA9urHtPK.1739472515372.4	13622716	34	36	36

-- TEST 3: look at specific visit that viewed many different shops, VISIT SPECIFIC 
-- visit_id	shops_viewed	listings_viewed	listing_views	purchased_after_view
-- xeY8fX3lgSFL0HGcDCucJu0HDveT.1737590512959.63	75	79	79	0
---------not in gms table bc did not buy anything
select
  -- lv.platform,
  visit_id,
  count(distinct shop_id) as shops_viewed,
  count(distinct listing_id) as listings_viewed,
  count(visit_id) as listing_views,
  sum(purchased_after_view) as purchased_after_view,
from 
  etsy-data-warehouse-prod.analytics.listing_views
inner join  
  active_listings using (listing_id)
where 
  _date >= current_date-30 
  and platform in ('mobile_web','desktop') 
  and referring_page_event in ('shop_home')
  and visit_id in ('xeY8fX3lgSFL0HGcDCucJu0HDveT.1737590512959.63') 
group by all 
-- visit_id	shops_viewed	listings_viewed	listing_views	purchased_after_view
-- xeY8fX3lgSFL0HGcDCucJu0HDveT.1737590512959.63	75	79	79	0

----- agg at shop_id level (limit bc all others are just 1 listing per shop)
-- visit_id	shop_id	listings_viewed	listing_views	purchased_after_view
-- xeY8fX3lgSFL0HGcDCucJu0HDveT.1737590512959.63	12676643	3	3	0
-- xeY8fX3lgSFL0HGcDCucJu0HDveT.1737590512959.63	8852820	2	2	0
-- xeY8fX3lgSFL0HGcDCucJu0HDveT.1737590512959.63	41819812	2	2	0
-- xeY8fX3lgSFL0HGcDCucJu0HDveT.1737590512959.63	6169843	1	1	0


----CEHCKING FULL CODE AT THIS LEVLE
visit_id	unique_listings_viewed	visits_view_listings_from_shop_home	shop_home_listing_views	gms_net
xeY8fX3lgSFL0HGcDCucJu0HDveT.1737590512959.63	1	1	72	
xeY8fX3lgSFL0HGcDCucJu0HDveT.1737590512959.63	3	1	3	
xeY8fX3lgSFL0HGcDCucJu0HDveT.1737590512959.63	2	1	4	
