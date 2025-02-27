------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- WHAT % OF SHOPS HAVE NULL SECTIONS? 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
with shop_visits as (
select
  shop_id,
  count(visit_id) as pageviews 
from 
  etsy-bigquery-adhoc-prod._script146821e037627cbc101047258e54b02ce7ae2a33.visited_shops
group by all 
)
, agg as (
select
  b.shop_id, -- active shops 
  shop_name,
  case when v.shop_id is not null then 1 else 0 end as visited,
  pageviews,
  case when s.shop_id is not null and active_listing_count > 0 then 1 else 0 end as has_sections_w_listings, 
  count(case when active_listing_count > 0 then name end) as number_of_sections_w_listings,
  sum(case when (name is null or name in ('')) and active_listing_count > 0 then 1 else 0 end) as empty_sections_with_listings,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
left join
  shop_visits v
    on v.shop_id=cast(b.shop_id as string) 
where 1=1
  and active_seller_status = 1 -- is an active seller 
group by all 
)
select
  count(distinct shop_id) as active_shops,
  sum(visited) as visited_shops,
  sum(pageviews) as pageviews,
  count(distinct case when empty_sections_with_listings > 0 then shop_id end) as active_shop_without_section_names,
  sum(case when empty_sections_with_listings > 0 then visited end) as visited_shop_without_section_names,
  sum(case when empty_sections_with_listings > 0 then pageviews end) as pageviews_without_section_names
from agg


-- begin 
-- create or replace temp table visited_shops as (
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
--   and platform in ('mobile_web','desktop')
--   and (beacon.event_name in ('shop_home'))
-- group by all
-- );
-- end

-- etsy-bigquery-adhoc-prod._script146821e037627cbc101047258e54b02ce7ae2a33.visited_shops
