------------------------------------------------------------------------------------------------------------------------------------------------------
-- do we have any data on % of shops that run shipping promotions and/or are eligible for free shipping guarantee?
------------------------------------------------------------------------------------------------------------------------------------------------------
-- create or replace table etsy-data-warehouse-dev.madelinecollins.web_shop_visits as (
-- select
--   platform,
--   beacon.event_name, 
--   (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id, 
--   (select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id, 
--   visit_id, 
--   sequence_number,
-- from
--   `etsy-visit-pipe-prod.canonical.visit_id_beacons`
-- inner join 
--   etsy-data-warehouse-prod.weblog.visits using (visit_id)
-- where
--   date(_partitiontime) >= current_date-30
--   and _date >= current_date-30
--   and platform in ('mobile_web','desktop','boe')
--   and (beacon.event_name in ('shop_home'))
-- group by all
-- );


-- shops with a free shipping guarantee
select 
  count(distinct case when buyer_promise_enabled = 1 then sb.shop_id end) as free_shipping_active_shops, -- this is the free shipping guarantee  
  count(distinct case when buyer_promise_enabled = 1 then v.shop_id end) as free_shipping_visited_shops,
  count(distinct sb.shop_id) as active_shops,
  count(distinct v.shop_id) as visited_shops,
from 
  `etsy-data-warehouse-prod.rollups.seller_basics` sb
left join 
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits v 
    on cast(sb.shop_id as string)=v.shop_id 
where is_frozen = 0
  and active_seller_status = 1

-- create or replace table etsy-data-warehouse-dev.madelinecollins.web_shop_visits as (
-- select
--   platform,
--   beacon.event_name, 
--   (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id, 
--   (select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id, 
--   visit_id, 
--   sequence_number,
-- from
--   `etsy-visit-pipe-prod.canonical.visit_id_beacons`
-- inner join 
--   etsy-data-warehouse-prod.weblog.visits using (visit_id)
-- where
--   date(_partitiontime) >= current_date-30
--   and _date >= current_date-30
--   and platform in ('mobile_web','desktop','boe')
--   and (beacon.event_name in ('shop_home'))
-- group by all
-- );


/* all metrics, free shipping */
select 
  seller_tier_new,
  -- shops 
  count(distinct case when buyer_promise_enabled = 1 then sb.shop_id end) as free_shipping_active_shops, -- this is the free shipping guarantee  
  count(distinct case when buyer_promise_enabled = 1 then v.shop_id end) as free_shipping_visited_shops,
  count(distinct sb.shop_id) as active_shops,
  count(distinct v.shop_id) as visited_shops,
  -- these are all for visited shops  
  count(case when buyer_promise_enabled = 1 then sequence_number end) as free_shipping_pageviews, 
  count(sequence_number) as visited_pageviews, 
  count(distinct case when buyer_promise_enabled = 1 then v.visit_id end) as free_shipping_visits, -- this is the free shipping guarantee  
  count(distinct v.visit_id) as shop_home_visits,
from 
  `etsy-data-warehouse-prod.rollups.seller_basics` sb
left join 
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits v 
    on cast(sb.shop_id as string)=v.shop_id 
where 1=1
  and is_frozen = 0
  and active_seller_status = 1
group by all


-- shipping promotion
with all_shops as (
select  
  b.shop_id, -- all active shops
  case when promotion_type in (3, 6, 7, 8) then 1 else 0 end as shops_w_shipping_promotions -- promotions opted into 
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  etsy-data-warehouse-prod.etsy_shard.seller_marketing_promotion p using (shop_id)
where 
  is_frozen = 0
  and active_seller_status = 1
)
select
  count(distinct a.shop_id) as active_shops,
  count(distinct v.shop_id) as visited_shops,
  count(distinct case when shops_w_shipping_promotions = 1 then a.shop_id end) as promotion_active_shops,
  count(distinct case when shops_w_shipping_promotions = 1 then v.shop_id end) as promotion_visited_shops,
from 
  all_shops a
left join 
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits v 
    on cast(a.shop_id as string)=v.shop_id 

/* all metrics, shipping promotion */
-- select shop_id, count(*) from etsy-bigquery-adhoc-prod._script7132dc76c99a23f7f56e3f1dc5b738aac2790b6d.all_shops group by all order by 2 desc limit 5

-- begin
-- create or replace temp table all_shops as (
-- select  
--   seller_tier_new,
--   b.shop_id, -- all active shops
--   MAX(case when promotion_type in (3, 6, 7, 8) then 1 else 0 end) as shops_w_shipping_promotions -- promotions opted into 
-- from 
--   etsy-data-warehouse-prod.rollups.seller_basics b
-- left join 
--   etsy-data-warehouse-prod.etsy_shard.seller_marketing_promotion p using (shop_id)
-- where 
--   is_frozen = 0
--   and active_seller_status = 1
-- group by all
-- );
-- end
-- --etsy-bigquery-adhoc-prod._script7132dc76c99a23f7f56e3f1dc5b738aac2790b6d.all_shops

select 
  seller_tier_new,
  -- shops 
  count(distinct case when shops_w_shipping_promotions = 1 then sb.shop_id end) as shipping_promo_active_shops, -- this is the free shipping guarantee  
  count(distinct case when shops_w_shipping_promotions = 1 then v.shop_id end) as shipping_promo_visited_shops,
  count(distinct sb.shop_id) as active_shops,
  count(distinct v.shop_id) as visited_shops,
  -- these are all for visited shops  
  count(case when shops_w_shipping_promotions = 1 then sequence_number end) as shipping_promo_pageviews, 
  count(sequence_number) as visited_pageviews, 
  count(distinct case when shops_w_shipping_promotions = 1 then v.visit_id end) as shipping_promo_visits, -- this is the free shipping guarantee  
  count(distinct v.visit_id) as shop_home_visits,
from 
  etsy-bigquery-adhoc-prod._script7132dc76c99a23f7f56e3f1dc5b738aac2790b6d.all_shops sb
left join 
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits v 
    on cast(sb.shop_id as string)=v.shop_id 
group by all
