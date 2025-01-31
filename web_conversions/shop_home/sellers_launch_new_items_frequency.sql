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
  visit_id
	, sequence_number
	, (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id
from 
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` 
where 
  beacon.event_name in ('shop_home')
  and date(_partitiontime) >= current_date-30
)
, active_listings as (
select
  shop_id, 
  count(distinct case when create_date <= current_date-30 then listing_id end) as new_item,
  count(distinct case when create_date > current_date-30 then listing_id end) as old_item,
  count(distinct listing_id) as total_listings
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics
group by all 
)
