select
  case when regexp_contains(title, r'\b(free shipping|shipping fee|express shipping|express delivery|upgrade shipping)\b') then 1 else 0 end as exact_shipping_title,
  case when regexp_contains(title, r'\b(shipping|delivery)\b') then 1 else 0 end as general_shipping_title,
  count(distinct listing_id) as listings,
  count(distinct shop_id) as shops,
  count(v.visit_id) as listing_views
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics b
left join 
  etsy-data-warehouse-prod.analytics.listing_views v using (listing_id)
where 
  v._date >= current_date-30
group by all 

--testing
select
  title,
  case when regexp_contains(title, r'\b(free shipping|shipping fee|express shipping|express delivery|upgrade shipping)\b') then 1 else 0 end as exact_shipping_title,
  case when regexp_contains(title, r'\b(shipping|delivery)\b') then 1 else 0 end as general_shipping_title,
  count(distinct listing_id) as listings,
  count(distinct shop_id) as shops,
  count(v.visit_id) as listing_views
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics b
left join 
  etsy-data-warehouse-prod.analytics.listing_views v using (listing_id)
where 
  v._date >= current_date-30
  and  regexp_contains(title, r'\b(free shipping|shipping fee|express shipping|express delivery|upgrade shipping)\b') 
group by all 

limit 5
