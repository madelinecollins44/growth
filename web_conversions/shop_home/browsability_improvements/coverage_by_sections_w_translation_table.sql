with shop_sections as ( -- active shops + if they have sections with listings in them 
select 
  b.shop_id,
  b.seller_tier_new,
  shop_name,
  b.user_id as seller_user_id, 
  count(distinct case when active_listing_count > 0 then coalesce(s.name,t.name) end) as sections
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections_translations t 
    on s.shop_id=t.shop_id
    and s.id=t.id
where
  active_seller_status = 1
group by all
)
select
  sections,
  seller_tier_new,
  count(distinct shop_id) as shops
from shop_sections
group by all


--------------------------------------------------
--TESTING
--------------------------------------------------
with shop_sections as ( -- active shops + if they have sections with listings in them 
select 
--s.*,t.*
  b.shop_id,
  b.seller_tier_new,
  shop_name,
  b.user_id as seller_user_id, 
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
where
  active_seller_status = 1
  and s.shop_id in (46345829)
group by all
)
select * from shop_sections where shop_id in (46345829)
-- select
--   sections,
--   seller_tier_new,
--   count(distinct shop_id) as shops
-- from shop_sections
-- group by all
