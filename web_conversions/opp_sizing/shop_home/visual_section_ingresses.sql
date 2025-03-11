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
-- OVERALL TRAFFIC COUNTS 
---------------------------------------------------------------
-- get shops with at least 4 sections with at least 3 listings in each section 
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
  case when (s.shop_id is not null or t.shop_id is not null) then 1 else 0 end as has_sections,
  count(case when active_listing_count >= 3 then s.id end) as sections_w_3_listings,
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
