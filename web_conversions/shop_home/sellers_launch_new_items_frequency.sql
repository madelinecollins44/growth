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
from 
  active_shops
left join 
  active_shops

----------------------------------------------------------------------------------------------------------------
--TESTING
----------------------------------------------------------------------------------------------------------------
-- TEST 1: making sure new vs old items is working
select
  shop_id, 
  count(distinct case when create_date <= current_date-30 then listing_id end) as new_item,
  count(distinct case when create_date > current_date-30 then listing_id end) as old_item,
  count(distinct listing_id) as total_listings
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics
group by all 
having total_listings = 40
-- shop_id	new_item	old_item	total_listings
-- 24218839	54582	18035	72617
-- 5400716	42974	18059	61033
-- 54681694	53931	998	54929
-- 14280094	38336	11632	49968
-- 16495683	30219	7683	37902
-- 16405631	23925	10889	34814
-- 7402891	26038	7159	33197
-- 5413707	24317	8429	32746
-- 41683802	27069	3841	30910
-- 45802006	22948	7701	30649

-- testing to see where total listing count is 40, making sure the dates all line up 
-- shop_id	new_item	old_item	total_listings
-- 57146567	0	40	40
-- 56210004	29	11	40
-- 52475632	34	6	40
-- 45390271	32	8	40
-- 20019059	24	16	40

select * from etsy-data-warehouse-prod.rollups.active_listing_basics where shop_id = 57146567
