---------------------------------------------------------------
-- OVERALL TRAFFIC COUNTS 
---------------------------------------------------------------
select
  count(distinct visit_id) as total_visits,
  count(distinct case when converted > 0 then visit_id end) as converted,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_visits,  
  count(distinct case when platform in ('desktop') and converted > 0 then visit_id end) as desktop_converted,
  count(distinct case when platform in ('mobile_web') then visit_id end) as mobile_web_visits,  
  count(distinct case when platform in ('mobile_web') and converted > 0 then visit_id end) as mobile_web_converted
from 
  etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30

-- visits with shop home
select
  count(distinct visit_id) as total_visits,
  count(distinct case when converted > 0 then visit_id end) as converted,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_visits,  
  count(distinct case when platform in ('desktop') and converted > 0 then visit_id end) as desktop_converted,
  count(distinct case when platform in ('mobile_web') then visit_id end) as mobile_web_visits,  
  count(distinct case when platform in ('mobile_web') and converted > 0 then visit_id end) as mobile_web_converted
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 
  v._date >= current_date-30
  and e.event_type in ('shop_home')

  
---------------------------------------------------------------
-- TRAFFIC TO SHOPS WITH 4+ SECTIONS WITH 3+ LISTNGS IN EACH
---------------------------------------------------------------
-- -- get shops with at least 4 sections with at least 3 listings in each section 
-- begin
-- create or replace temp table shops_w_4_sections as (
-- with translated_sections as ( -- grab english translations, or whatever translation is set to 1
-- select 
--   *
-- from etsy-data-warehouse-prod.etsy_shard.shop_sections_translations
-- qualify row_number() over (
--     partition by id 
--     order by
--         case when language = 5 then 1 else 2 end,  -- Prioritize language = 5
--         language asc  -- If no language = 5, take the lowest language number
-- ) = 1
-- )
-- , shop_sections as (
-- select 
--   b.shop_id,
--   b.user_id as seller_user_id,
--   shop_name,
--   case when (s.shop_id is not null or t.shop_id is not null) then 1 else 0 end as has_sections,
--   count(case when active_listing_count >= 3 then s.id end) as sections_w_3_listings,
-- from 
--   etsy-data-warehouse-prod.rollups.seller_basics b
-- left join 
--   etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
-- left join 
--   translated_sections t 
--     on s.shop_id=t.shop_id
--     and s.id=t.id
-- where
--   active_seller_status = 1 -- active sellers
--   and is_frozen = 0  -- not frozen accounts 
--   and active_listings > 0 -- shops with active listings
-- group by all
-- )
-- select 
--   shop_id, 
--   sections_w_3_listings
-- from 
--   shop_sections 
-- where 
--   sections_w_3_listings >= 4
-- );
-- end
-- etsy-bigquery-adhoc-prod._script78b8cdbea7896696f8aea4f016f1216ed6385e29.shops_w_4_sections

-- -- visits info 
-- begin
-- create or replace temp table shop_home_visits as (
-- select
--   platform,
--   beacon.event_name, 
--   (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id, 
--   (select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id, 
--   visit_id, 
--   sequence_number,
--   converted,
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
-----etsy-bigquery-adhoc-prod._script21c9b882b08f76514ed1fe83477a64aff1366131.shop_home_visits


select
  count(distinct visit_id) as unique_visits,
  count(distinct case when converted > 0 then visit_id end) as converted_visits
from v
inner join 
  etsy-bigquery-adhoc-prod._script78b8cdbea7896696f8aea4f016f1216ed6385e29.shops_w_4_sections s
    on v.shop_id=cast(s.shop_id as string)
where v.platform in ('mobile_web','desktop')
