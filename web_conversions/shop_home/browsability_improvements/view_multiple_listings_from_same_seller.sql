---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- QUESTION 1: What % of users on web view multiple listings from the same shop from Shop home? 
---- Use case - As a buyer viewing multiple listings from the same shop, I am able to easily access my most recently viewed listings from that shop. 
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------
-- OVERALL TRAFFIC BY SELLER STATUS
----------------------------------------------------------
select
  v.platform,
  mu.is_seller, 
  count(distinct v.visit_id) as visits_from_non_sellers,
  count(distinct case when converted >0 then v.visit_id end) as visit_conversions,
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile mu
    using (user_id)
where
  _date >= current_date-30
  and v.platform in ('desktop','mobile_web','boe')
group by all
order by 1 asc
----------------------------------------------------------
-- OVERALL SHOP HOME TRAFFIC FROM NON-SELLERS
----------------------------------------------------------
with non_seller_visits as ( -- only look at visits from non- sellers
select
  v.platform,
  v.visit_id,
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile mu
    using (user_id)
where
  mu.is_seller = 0 
  and platform in ('mobile_web','desktop')
  and _date >= current_date-30
)
select
  count(distinct e.visit_id) as total_visits,
  count(distinct case when event_type in ('shop_home') then e.visit_id end) as shop_home_visits
from etsy-data-warehouse-prod.weblog.events e
inner join non_seller_visits nsv using (visit_id)

----------------------------------------------------------
-- SHOP HOME SPECIFIC LISTING VIEWS BY SHOP_ID
----------------------------------------------------------
with non_seller_visits as ( -- only look at visits from non- sellers
select
  v.platform,
  v.visit_id,
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile mu
    using (user_id)
where
  mu.is_seller = 0 
  and _date >= current_date-30
)
, active_listings as ( -- need this to pull in shop_id
select
  listing_id,
  shop_id
from etsy-data-warehouse-prod.rollups.active_listing_basics
)
, shop_home_listing_views as ( -- start with pulling all data on listing views from shop_home page. this is all at the shop_id level. 
select
  nsv.platform,
  nsv.visit_id,
  al.shop_id,
  count(distinct lv.listing_id) as unique_listings_viewed,
  count(lv.visit_id) as listing_views,
  sum(lv.purchased_after_view) as purchased_after_view,
from 
  non_seller_visits nsv
inner join 
  etsy-data-warehouse-prod.analytics.listing_views lv
    using (visit_id)
inner join 
  active_listings al
    on lv.listing_id=al.listing_id
where 
  _date = current_date-30
  and lv.platform in ('desktop','mobile_web','boe')
  and referring_page_event in ('shop_home') -- only looking at active shop home pages 
group by all 
)
select  
  platform,
  count(distinct visit_id) as visits_view_listings_from_shop_home,
  sum(listing_views) as shop_home_listing_views,
-- only 1 listing per seller viewed
  count(distinct case when unique_listings_viewed = 1 then visit_id end) as visits_view_1_listing,
  sum(case when unique_listings_viewed = 1 then listing_views end) as sh_listing_views_from_1_listing_per_seller,
  sum(case when unique_listings_viewed = 1 then purchased_after_view end) as sh_purchases_from_1_listing_per_seller,
-- 1+ listing per seller viewed
  count(distinct case when unique_listings_viewed > 1 then visit_id end) as visits_view_1_plus_listings,
  sum(case when unique_listings_viewed > 1  then listing_views end) as sh_listing_views_from_1_plus_listing_per_seller,
  sum(case when unique_listings_viewed > 1 then purchased_after_view end) as sh_purchases_from_1_plus_listing_per_seller,
from 
  shop_home_listing_views
group by all 


------------------------------------
-- OVERALL METRICS TO COMPARE
------------------------------------  
with non_seller_visits as ( -- only look at visits from non- sellers
select
  v.platform,
  v.visit_id,
  v.converted,
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile mu
    using (user_id)
where
  mu.is_seller = 0 
  and _date >= current_date-30
  and v.platform in ('desktop','mobile_web','boe')
)
select
  nsv.platform,
  --overall metrics
  count(distinct nsv.visit_id) as visits_from_non_sellers,
  count(distinct listing_id) as unique_listings_viewed,
  count(distinct lv.visit_id) as visits_lv,
  count(distinct case when converted > 0 then nsv.visit_id end) as visit_conversions,
  count(distinct case when converted > 0 then lv.visit_id end) as visit_lv_conversions,
  count(visit_id) as total_lv,
  sum(purchased_after_view) as total_cr,
  -- shop home metrics
  count(distinct case when referring_page_event in ('shop_home') then listing_id end) as sh_unique_listings_viewed,
  count(distinct case when referring_page_event in ('shop_home') then lv.visit_id end) as sh_visits_lv,
  count(distinct case when purchased_after_view > 0 and referring_page_event in ('shop_home') then lv.visit_id end) as sh_visit_conversions,
  count(case when referring_page_event in ('shop_home') then visit_id end) as sh_total_lv,
  sum(case when referring_page_event in ('shop_home') then purchased_after_view end) as sh_total_cr
from 
  non_seller_visits nsv
left join 
  (select * from etsy-data-warehouse-prod.analytics.listing_views where _date = current_date-30) lv
    using (visit_id)
group by all 

------------------------------------
-- TESTING
------------------------------------  
-- // TEST 1: make sure # of views for each seller makes sense //
with non_seller_visits as ( -- only look at visits from non- sellers
select
  v.platform,
  v.visit_id,
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile mu
    using (user_id)
where
  mu.is_seller = 0 
  and _date >= current_date-30
)
, shop_home_listing_views as ( -- start with pulling all data on listing views from shop_home page
select
  nsv.platform,
  nsv.visit_id,
  seller_user_id, -- used to distinguish between each seller's shop home
  count(distinct listing_id) as unique_listings_viewed,
  count(visit_id) as listing_views,
  sum(added_to_cart) as added_to_cart,
  sum(favorited) as favorited,
  sum(purchased_after_view) as purchased_after_view,
from 
  non_seller_visits nsv
inner join 
  etsy-data-warehouse-prod.analytics.listing_views lv
    using (visit_id)
where 
  _date = current_date-30
  and lv.platform in ('desktop','mobile_web','boe')
  and referring_page_event in ('shop_home') -- only looking at active shop home pages 
group by all 
)
select
  visit_id,
  seller_user_id,
  listing_views,
from shop_home_listing_views
group by all 
order by 3 desc
limit 5


-- // TEST 2: see how many listing views have a null seller_user_id //
with non_seller_visits as ( -- only look at visits from non- sellers
select
  v.platform,
  v.visit_id,
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile mu
    using (user_id)
where
  mu.is_seller = 0 
  and _date >= current_date-30
)
, shop_home_listing_views as ( -- start with pulling all data on listing views from shop_home page
select
  nsv.platform,
  nsv.visit_id,
  seller_user_id, -- used to distinguish between each seller's shop home
  count(distinct listing_id) as unique_listings_viewed,
  count(visit_id) as listing_views,
  sum(added_to_cart) as added_to_cart,
  sum(favorited) as favorited,
  sum(purchased_after_view) as purchased_after_view,
from 
  non_seller_visits nsv
inner join 
  etsy-data-warehouse-prod.analytics.listing_views lv
    using (visit_id)
where 
  _date = current_date-30
  and lv.platform in ('desktop','mobile_web','boe')
  and referring_page_event in ('shop_home') -- only looking at active shop home pages 
group by all 
)
select 
  sum(case when seller_user_id is null then listing_views end) as null_seller_lv,
  sum(case when seller_user_id is not null then listing_views end) as not_null_seller_lv,
  sum(listing_views) as total_lv,
  sum(case when seller_user_id is null then listing_views end) / sum(listing_views) as share_wo_seller_id,
  sum(case when seller_user_id is not null then listing_views end) / sum(listing_views) as share_w_seller_id
from shop_home_listing_views
group by all 
-- null_seller_lv	not_null_seller_lv	total_lv	share_wo_seller_id	share_w_seller_id
-- 3080295	1399345	4479640	0.68762110348152972	0.31237889651847023

-- what % of active listings are missing shop_ids? need to use active_listings to get shop_id, not analytics.listing_views
select 
  count(distinct case when shop_id is null then listing_id end) as null_seller_lv,
  count(distinct case when shop_id is not null then listing_id end) as not_null_seller_lv,
  count(distinct listing_id) as total_lv,
  count(distinct case when shop_id is null then listing_id end) / count(distinct listing_id) as share_wo_seller_id,
  count(distinct case when shop_id is not null then listing_id end) / count(distinct listing_id) as share_w_seller_id
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics
-- null_seller_lv	not_null_seller_lv	total_lv	share_wo_seller_id	share_w_seller_id
-- 0	127414390	127414390	0.0	1.0

-- // TEST 3: confirm shop_id from shop_home page matches shop_id from the next view listing //
----grab the shop_ids from listings viewed with a shop_home referrering event
--check to see what the shop_id for listing views are, and then check to see if that visit looked at the shop_home page before that
--check to see what the shop_id for listing views are, and then check to see if that visit looked at the shop_home page before that
with shop_home_listing_views as (
select
  visit_id,
  sequence_number,
  listing_id,
from 
  etsy-data-warehouse-prod.analytics.listing_views 
where
  _date = current_date-5
  and referring_page_event in ('shop_home') 
  and platform in ('mobile_web','desktop')
) 
, active_listings as (
select
  listing_id,
  shop_id
from etsy-data-warehouse-prod.rollups.active_listing_basics
)
select
  visit_id,
  sequence_number,
  sh.listing_id,
  shop_id,
from shop_home_listing_views sh
inner join active_listings al using (listing_id)
limit 5
-- visit_id	sequence_number	listing_id	shop_id
-- A9fc2OSqewadEeedcztpKLelH26b.1738895990958.1	52	1722385305	47051984
-- A9uqjVNpL24IOigimHAIiCir94um.1738970913666.1	67	992245649	20890585
-- A9T1gCslr7W5XyfyTKbh-cGct3gM.1738906392815.1	194	1702606003	51097370
-- A9uqjVNpL24IOigimHAIiCir94um.1738970913666.1	108	992245649	20890585
-- A8vPTa4PlZUxf_jXRfxjlAz3qRLj.1738918517861.1	690	1676413065	35219554

-----confirm that shop_ids were seen in shop home event before that listing view : confirmed
select
  beacon.event_name,
  (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id,
  (select value from unnest(beacon.properties.key_value) where key = "listing_id") as listing_id,
  visit_id, 
  sequence_number
from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where
		date(_partitiontime) >= current_date-5
    and (beacon.event_name in ('shop_home','view_listing'))    -- looking at favoriting on shop_home page
    and visit_id in ('A9fc2OSqewadEeedcztpKLelH26b.1738895990958.1')
group by all
order by sequence_number
-- event_name	shop_id	listing_id	visit_id	sequence_number
-- view_listing	47051984	1564321874	A9fc2OSqewadEeedcztpKLelH26b.1738895990958.1	0
-- shop_home	47051984		A9fc2OSqewadEeedcztpKLelH26b.1738895990958.1	32
-- view_listing	47051984	1722385305	A9fc2OSqewadEeedcztpKLelH26b.1738895990958.1	52
-- shop_home	47051984		A9fc2OSqewadEeedcztpKLelH26b.1738895990958.1	110

---do another test with a visit that viewed multiple shops 
with shop_home_listing_views as (
select
  visit_id,
  sequence_number,
  listing_id,
from 
  etsy-data-warehouse-prod.analytics.listing_views 
where
  _date = current_date-5
  and referring_page_event in ('shop_home') 
  and platform in ('mobile_web','desktop')
) 
, active_listings as (
select
  listing_id,
  shop_id
from etsy-data-warehouse-prod.rollups.active_listing_basics
)
select
  visit_id,
  count(distinct shop_id) as shops_visited
from shop_home_listing_views sh
inner join active_listings al using (listing_id)
group by all
order by 2 desc
limit 5
-- visit_id	shops_visited
-- 1YojelzvWWKoTwwYPcwp14Fij2ju.1738874613924.1	28
-- 5eebjt7CPgY7gP4JrxhDv16oLf4v.1738941790812.3	23
-- 0AP5rVISIvxAncpeUb9xaDTC6P20.1738915014143.1	23
-- PdEcLGQ71fRkBKI8KLh-3TyMchNm.1738882493098.2	23
-- cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	22

--get all shop home listing views for cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	, form analytics.listing_views 
-- visit_id	sequence_number	listing_id	shop_id
-- cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	3	902966549	25591439
-- cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	56	1228627904	25591439
-- cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	79	1242583729	25591439
-- cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	103	1100559145	25591439
-- cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	188	1573529135	25591439
-- cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	294	1845420162	25580850
-- cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	318	1778025973	25580850
-- cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	347	1060290993	25580850
-- cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	368	1527260470	25580850
-- cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	398	1541445897	25580850
-- cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	430	1609957645	37850210
-- cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	451	1595793190	37850210
-- cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	470	1690229191	37850210
-- cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	487	1595795468	37850210
-- cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	507	1355245390	37850210

--get all shop_home + listing view events for  
select
  beacon.event_name,
	lag(beacon.event_name) over (partition by visit_id order by sequence_number) as previous_event,
  lag(sequence_number) over (partition by visit_id order by sequence_number) as previous_sequence_number,
  (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id,
  (select value from unnest(beacon.properties.key_value) where key = "listing_id") as listing_id,
  visit_id, 
  sequence_number
from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where
		date(_partitiontime) >= current_date-5
    and (beacon.event_name in ('shop_home','view_listing'))    -- looking at favoriting on shop_home page
    and visit_id in ('cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2')
  group by all
order by sequence_number asc
limit 15
-- event_name	previous_event	previous_sequence_number	shop_id	listing_id	visit_id	sequence_number
-- view_listing			25591439	902966549	cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	3
-- view_listing	view_listing	3	25591439	1228627904	cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	56
-- view_listing	view_listing	56	25591439	1242583729	cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	79
-- view_listing	view_listing	79	25591439	1100559145	cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	103
-- view_listing	view_listing	103	25580850	1381217224	cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	137
-- view_listing	view_listing	137	24653307	1066002748	cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	161
-- view_listing	view_listing	161	25591439	1573529135	cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	188
-- view_listing	view_listing	188	25591439	1852761553	cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	236
-- shop_home	view_listing	236	25580850		cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	290
-- view_listing	shop_home	290	25580850	1845420162	cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	294
view_listing	view_listing	294	25580850	1778025973	cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	318
view_listing	view_listing	318	25580850	1060290993	cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	347
view_listing	view_listing	347	25580850	1527260470	cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	368
view_listing	view_listing	368	25580850	1541445897	cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	398
shop_home	view_listing	398	24653307		cYRWndVuVkU1WkRXwOxiUgB0pDk1.1738941383457.2	426
