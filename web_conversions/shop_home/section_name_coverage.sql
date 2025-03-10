----------------------------------------------------------------------
-- RUN QUERY TO GET SHOP HOME VISIT INFO 
----------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.web_shop_visits as (
select
  platform,
  beacon.event_name, 
  (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id, 
  (select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id, 
  visit_id, 
  sequence_number,
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
inner join 
  etsy-data-warehouse-prod.weblog.visits using (visit_id)
where
  date(_partitiontime) >= current_date-30
  and _date >= current_date-30
  and platform in ('mobile_web','desktop','boe')
  and (beacon.event_name in ('shop_home'))
group by all
);

--------------------------------------------------
-- CREATE TABLE TO GET SECTIONS FOR ALL SHOPS
--------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.active_shops_and_section_info as (
with translated_sections as ( -- grab english translations, or whatever translation is set to 1
select 
  *
from etsy-data-warehouse-prod.etsy_shard.shop_sections_translations
qualify row_number() over (
    partition by id 
    order by
        case when language = 5 then 1 else 2 end,  -- Prioritize language = 5
        language asc  -- If no language = 5, take the lowest language number
) = 1
)
select 
  b.shop_id,
  b.user_id as seller_user_id,
  shop_name,
  coalesce(is_etsy_plus,0) as is_etsy_plus,
  seller_tier_new,
  -- case when (s.shop_id is not null or t.shop_id is not null) and active_listing_count > 0 then 1 else 0 end as has_sections_w_listings,
  case when (s.shop_id is not null or t.shop_id is not null) then 1 else 0 end as has_sections,
  count(s.id) as sections,
  count(case when active_listing_count > 0 then s.id end) as sections_w_listings,
  count(case when ((coalesce(nullif(s.name, ''),t.name)) is not null) and active_listing_count > 0 then s.id end) as filled_ids,
  count(case when ((coalesce(nullif(s.name, ''),t.name)) is null) and active_listing_count > 0 then s.id end) as missing_ids,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
left join 
  translated_sections t 
    on s.shop_id=t.shop_id
    and s.id=t.id
where
  active_seller_status = 1 -- active sellers
  and is_frozen = 0  -- not frozen accounts 
  and active_listings > 0 -- shops with active listings
group by all
);

--------------------------------------------------------
-- WHAT TYPES OF SECTIONS ARE SHOPS USING? 
--------------------------------------------------------
