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
  case when s.shop_id is not null or t.shop_id is not null then 1 else 0 end as has_sections,
  count(case when active_listing_count > 0 then s.id end) as sections,
  count(case when (coalesce(nullif(s.name, ''),t.name)) is not null then s.id end) as filled_ids,
  count(case when (coalesce(nullif(s.name, ''),t.name)) is null then s.id end) as missing_ids,
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
)
select
  count(distinct shop_id) as active_shops,
  count(distinct case when has_sections > 0 then shop_id end) as shops_w_sections_hs,
  count(distinct case when has_sections = 0 then shop_id end) as shops_wo_sections_hs,
  count(distinct case when sections > 0 then shop_id end) as shops_w_sections,
  count(distinct case when sections = 0 then shop_id end) as shops_wo_sections,
from section_count

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
