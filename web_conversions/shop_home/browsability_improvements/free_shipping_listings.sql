with shop_sections as (
select 
  shop_id,
  user_id as seller_user_id, 
  count(distinct name) as sections
from 
  etsy-data-warehouse-prod.etsy_shard.shop_sections
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics using (shop_id)
where
  active_listing_count > 0 
  and active_seller_status = 1
group by all
)
, shop_gms as ( -- gms for sellers over last 
select
  seller_user_id,
  sum(gms_net) as total_gms
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans
where 
  date >= current_date-365
group by all 
)
, shop_visits as (
select
  beacon.event_name, 
  (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id, 
  (select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id, 
  count(visit_id) as views, 
  count(distinct visit_id) as visits
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
  date(_partitiontime) >= current_date-30
  and beacon.event_source in ('web')
  and (beacon.event_name in ('shop_home'))
group by all
)
, total_visits as (
select 
  count(distinct visit_id) as total_visits,
  sum(total_gms) as total_gms
from `etsy-data-warehouse-prod.weblog.visits`
where _date >= current_date-30
and platform in ('desktop', 'mobile_web')
)
select
  case when sections > 0 then 1 else 0 end as has_sections,
  count(distinct v.seller_user_id) as visited_shops,
  sum(g.total_gms) as shop_gms
from 
  shop_visits v
left join 
  shop_gms g 
    on v.seller_user_id=cast(g.seller_user_id as string)
left join 
  shop_sections s
    on v.seller_user_id=cast(s.seller_user_id as string)
group by all
