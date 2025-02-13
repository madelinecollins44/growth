select
  case when regexp_contains(title, r'\b(free shipping|shipping fee|express shipping|express delivery|upgrade shipping)\b') then 1 else 0 end as exact_shipping_title,
  case when regexp_contains(title, r'\b(shipping|delivery)\b') then 1 else 0 end as general_shipping_title,
  count(distinct listing_id) as listings,
  count(distinct shop_id) as shops
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics
group by all 
