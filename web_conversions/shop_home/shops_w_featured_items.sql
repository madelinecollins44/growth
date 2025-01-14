----------------------------------------------------------------------------------------------------------------
--What % shops have featured items in categories, as opposed to just featured items? 
-- shop w/ categorized featured items:https://www.etsy.com/shop/MOENAStudio
-- shop w/ uncategorized featured items: https://www.etsy.com/shop/TiradiaCork
----------------------------------------------------------------------------------------------------------------
with shop_w_categories as (
select
  distinct shop_id
from 
  etsy-data-warehouse-prod.etsy_shard.shop_sections
where 
  featured_rank != -1 -- looks at shops with categorized featured listings
)
select
  count(distinct sb.shop_id) as active_shops,
  count(distinct swc.shop_id) as active_shops_w_categorized_features_listings
from 
  etsy-data-warehouse-prod.rollups.seller_basics sb
left join 
  shop_w_categories swc using (shop_id)
where
  active_seller_status = 1 -- only active sellers 

