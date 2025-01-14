----------------------------------------------------------------------------------------------------------------
--What % shops have featured items in categories, as opposed to just featured items? 
-- shop w/ categorized featured items:https://www.etsy.com/shop/MOENAStudio, 31547401
-- shop w/ uncategorized featured items: https://www.etsy.com/shop/TiradiaCork, 25742633
-- shops w both: https://www.etsy.com/shop/TheInclusiveGiftco, 30378868
----------------------------------------------------------------------------------------------------------------
with featured_count as (
select
  shop_id,
  case 
    when featured_products_layout = 0 then 4
    when featured_products_layout = 1 then 5
    else 0
  end as featured_products_layout -- this is used to figure out how many featured items each shop has
from 
  etsy-data-warehouse-prod.etsy_shard.shop_data
)
, features_sections as (
select 
    distinct shop_id
  from 
    etsy-data-warehouse-prod.etsy_shard.shop_sections
  left join 
    featured_count using (shop_id)
  where 
    featured_rank != -1 -- looks at shops with categorized featured listings
    AND featured_rank < featured_products_layout
)
, features_item as (
select
  distinct shop_id 
from 
  etsy-data-warehouse-prod.etsy_shard.listings
left join 
  featured_count using (shop_id)
where 
  featured_rank != - 1 and
  featured_rank < featured_products_layout
  and is_available != 0 
  and is_displayable != 0
)
, shop_agg as (
  select
  sb.shop_id, 
  case when fs.shop_id is not null then 1 else 0 end as features_section, 
  case when fi.shop_id is not null then 1 else 0 end as features_item, 
from 
  etsy-data-warehouse-prod.rollups.seller_basics sb
left join 
  features_sections fs using (shop_id)
left join 
  features_item fi on sb.shop_id=fi.shop_id
where
  sb.active_seller_status = 1 -- only active sellers 
)
select
  count(distinct shop_id) as total_active_shops,
  count(distinct case when features_section > 0 and features_item=0 then shop_id end) as shops_w_only_feature_section,
  count(distinct case when features_section=0 and features_item > 0 then shop_id end) as shops_w_only_feature_item,
  count(distinct case when features_section > 0 and features_item > 0 then shop_id end) as shops_w_both_features
from 
  shop_agg;
