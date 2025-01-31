----------------------------------------------------------------------------------------------------------------
--How often do sellers launch new items? What % of sellers have added a new item in the last 30 days?
----------------------------------------------------------------------------------------------------------------
with active_shops as (
select
  shop_id, 
  seller_tier_new,
  sum(active_listings) as active_listings
from 
  etsy-data-warehouse-prod.rollups.seller_basics
where 	
  active_seller_status = 1 -- only active sellers
group by all 
)
, visited_shops as (
select distinct
	(select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id,
  count(visit_id) as pageviews,
  count(distinct visit_id) as visits
from 
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` 
where 
  beacon.event_name in ('shop_home')
  and date(_partitiontime) >= current_date-30
)
, active_listings as (
select
  shop_id, 
  count(distinct case when days_since_original_create <= 30 then listing_id end) as new_item,
  count(distinct case when days_since_original_create > 30 then listing_id end) as old_item,
  count(distinct listing_id) as total_listings
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics
group by all 
)
select
  count(distinct a.shop_id) as active_shops,
  count(distinct v.shop_id) as visited_shops,
  count(distinct case when new_item > 0 then shop_id end) as shops_w_new_items,
from 
  active_shops a
left join 
  visited_shops v using (shop_id)
left join 
  active_listings al  
    on a.shop_id=al.shop_id

----------------------------------------------------------------------------------------------------------------
--TESTING
----------------------------------------------------------------------------------------------------------------
-- TEST 1: making sure new vs old items is working
select
  shop_id, 
  count(distinct case when days_since_original_create <= 30 then listing_id end) as new_item,
  count(distinct case when days_since_original_create > 30 then listing_id end) as old_item,
  count(distinct listing_id) as total_listings
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics
group by all 
having total_listings = 30 and new_item > 5
LIMIT 5
-- shop_id	new_item	old_item	total_listings
-- 50222235	6	24	30
-- 54575638	8	22	30
-- 44054319	15	15	30
-- 56244091	7	23	30
-- 56184015	7	23	30

-- testing to see where total listing count is 30, making sure the dates all line up 
select * from etsy-data-warehouse-prod.rollups.active_listing_basics where shop_id = 56244091 order by days_since_original_create
