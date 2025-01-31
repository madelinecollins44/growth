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
, visited_shops
, listings 
  case when create_date >= current_date-30 then 1 else 0 end as new_item, cont(listing_id) as listings
