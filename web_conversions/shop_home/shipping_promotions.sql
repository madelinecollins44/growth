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
