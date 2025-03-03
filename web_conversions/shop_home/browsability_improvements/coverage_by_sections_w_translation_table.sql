--------------------------------------------------
-- COMBINE SECTIONS TO GET GMS/ CR/ VISIT COVERAGE
--------------------------------------------------
with shop_sections as ( -- active shops + if they have sections with listings in them 
select 
  shop_id,
  seller_user_id,
  shop_name,
  sections_w_listings
from 
  etsy-bigquery-adhoc-prod._script32fb9713d90b49e109ed630f241e7819296ce9fa.active_shops_and_section_info
group by all
)
, shop_visits as ( -- visits that viewed a shop on web
select
  seller_user_id,
  visit_id,
  count(sequence_number) as views
from  
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits 
where 
  platform in ('desktop','mobile_web')
group by all 
)
, shop_gms_converts as ( -- get all shop info at the visit_id level
select
  g.seller_user_id,
  v.visit_id, 
  sum(gms_net) as gms_net,
from
  etsy-data-warehouse-prod.transaction_mart.transactions_visits v 
inner join
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans g
    on g.transaction_id=v.transaction_id 
where 
  g.date >= current_date-30 -- this will also have to be the last 30 days, since looking at a visit level 
group by all 
)
, shop_level as ( -- get everything to seller_user_id level 
select
  v.seller_user_id,
  count(distinct v.visit_id) as visits,
  count(distinct gc.visit_id) as converts, 
  sum(gms_net) as gms_net
from 
  shop_visits v
left join 
  shop_gms_converts gc 
    on v.seller_user_id= cast(gc.seller_user_id as string)
    and v.visit_id=gc.visit_id
group by all 
)
select
  case when s.sections_w_listings > 0 then 1 else 0 end as has_sections,
  count(distinct l.seller_user_id) as visited_shops,
  sum(visits) as visits,
  sum(converts) as converts,
  sum(gms_net) as gms_net
from 
  shop_sections s -- starting here to get all active shops, and then looking at whether or not those were visited. some shops that were visited are not active.
left join 
  shop_level l
    on l.seller_user_id= cast(s.seller_user_id as string)
group by all 

--------------------------------------------------
-- CREATE TABLE TO GET SECTIONS FOR ALL SHOPS
--------------------------------------------------
begin
create or replace temp table active_shops_and_section_info as (
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
  is_etsy_plus,
  seller_tier_new,
  case when (s.shop_id is not null or t.shop_id is not null) and active_listing_count > 0 then 1 else 0 end as has_sections_w_listings,
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
end
-- etsy-bigquery-adhoc-prod._script32fb9713d90b49e109ed630f241e7819296ce9fa.active_shops_and_section_info

--------------------------------------------------
--TESTING
--------------------------------------------------
-- TEST 1: check to make sure joining to tables make sense 
select 
--s.*,t.*
  b.shop_id,
  b.seller_tier_new,
  shop_name,
  b.user_id as seller_user_id, 
  -- coalesce(nullif(s.name, ''),t.name) ,
  case when active_listing_count > 0 then coalesce(nullif(s.name, ''),t.name) end as sections,
  count(distinct case when active_listing_count > 0 then coalesce(nullif(s.name, ''),t.name) end) as sections
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections_translations t 
    on s.shop_id=t.shop_id
    and s.id=t.id
    and s.create_date=t.create_date
where
  active_seller_status = 1
  and s.shop_id in (46345829)
group by all

-- TEST 2: make sure has_section count is accurate
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
, section_count as (
select 
  b.shop_id,
  shop_name,
  seller_tier_new,
  case when (s.shop_id is not null or t.shop_id is not null) and active_listing_count > 0 then 1 else 0 end as has_sections,
  count(case when active_listing_count > 0 then s.id end) as sections,
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
  -- and b.shop_id in (20077844)
group by all
)
select
*
from section_count
where has_sections = 1 and sections = 0 
