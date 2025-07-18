/* create or replace table etsy-data-warehouse-dev.madelinecollins.shop_basics as (
select
  basics.shop_id,
  count(distinct case when shop_data.branding_option != 0 then basics.shop_id end) as branding_banner, -- can we confirm 0 means this shop does not have branding? 
  count(distinct case when shop_data.message != ""  then basics.shop_id end) as annoucement, 
  count(distinct case when sections.shop_id is not null then basics.shop_id end) as shop_sections,
  count(distinct case when abt.shop_id is not null then basics.shop_id end) as about_section,
  count(distinct case when faq.shop_id is not null then basics.shop_id end) as faq_section,
  count(distinct case when share_items.shop_id is not null then basics.shop_id end) as updates,
  count(distinct case when personal_details.shop_id is not null then basics.shop_id end) as seller_details,
  count(distinct case when settings.name = 'machine_translation' and settings.value = 'off' then basics.shop_id end) as machine_translation,
  count(distinct case when settings.name = 'custom_orders_opt_in' and settings.value = 't' then basics.shop_id end) as accepts_custom_orders,
  count(distinct case when settings.name = 'hide_shop_home_page_sold_items' and settings.value = 'f' then basics.shop_id end) as show_sold_items, -- confirm that false means these are shown 
  count(distinct case when promoted_offer.shop_id is not null then basics.shop_id end) as offers_active_shop_coupon 
from 
  (select * from etsy-data-warehouse-prod.rollups.seller_basics where active_seller_status = 1) basics -- only looks at active shops
left join 
  (select * from etsy-data-warehouse-prod.etsy_shard.shop_data where status in ('active')) shop_data using (shop_id) -- only active shops 
left join 
    etsy-data-warehouse-prod.etsy_shard.shop_sections sections 
      on basics.shop_id=sections.shop_id
left join 
  (select 
    distinct shop_id 
  from 
    etsy-data-warehouse-prod.etsy_shard.shop_about 
  where 
    status in ('active')
    and not (coalesce(story, '') = '' and coalesce(story_headline, '') = '')
    ) abt  -- excludes inactive shops w/o text 
      on basics.shop_id=abt.shop_id
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_frequently_asked_questions faq
    on basics.shop_id=faq.shop_id
left join 
  (select * from etsy-data-warehouse-prod.etsy_shard.shop_share_items where is_deleted <> 1) share_items -- only looks at shops that currently have updates
    on basics.shop_id=share_items.shop_id
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_seller_personal_details personal_details -- what does details_id mean here? 
    on basics.shop_id=personal_details.shop_id
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_settings settings
    on basics.shop_id=settings.shop_id
left join 
  (select * from etsy-data-warehouse-prod.etsy_shard.seller_marketing_promoted_offer where is_active = 1) promoted_offer
    on basics.shop_id=promoted_offer.shop_id
group by all);
*/
------------------------------------------------------------------
-- WHAT % OF SHOPS HAVE ABOUT VARIOUS PAGE ELEMENTS?
------------------------------------------------------------------
with shop_reviews as ( -- this looks at all listings that have been purchased and whether or not they have a review
select 
  shop_id,
  seller_user_id,
  count(distinct transaction_id) as transactions,
  sum(has_review) as total_reviews
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
group by all 
)
select
  b.shop_id,
  shop_name,
  case when total_reviews = 0 or r.seller_user_id is null then 0 else 1 end as has_shop_reviews,
  -- case when transactions = 0 or r.seller_user_id is null then 0 else 1 end as has_transactions,
  count(distinct b.shop_id) as active_shops,
  count(distinct case when branding_banner >0 then b.shop_id end) as has_branding_banner,
  count(distinct case when annoucement >0 then b.shop_id end) as has_annoucement, 
  count(distinct case when shop_sections>0 then b.shop_id  end) as has_shop_sections,
  count(distinct case when about_section>0 then b.shop_id end) as has_about_section,
  count(distinct case when faq_section>0 then b.shop_id end) as has_faq_section,
  count(distinct case when updates>0 then b.shop_id  end) as has_updates,
  count(distinct case when seller_details>0 then b.shop_id  end) as has_seller_details,
  count(distinct case when machine_translation>0 then b.shop_id end) as mhas_achine_translation,
  count(distinct case when accepts_custom_orders>0 then b.shop_id end) as has_accepts_custom_orders,
  count(distinct case when show_sold_items>0 then b.shop_id end) as has_show_sold_items, -- confirm that false means these are shown 
  count(distinct case when offers_active_shop_coupon > 0 then b.shop_id end) as has_offers_active_shop_coupon 
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  shop_reviews r
   using (shop_id)
left join 
  etsy-data-warehouse-dev.madelinecollins.shop_basics sb 
    on b.shop_id=sb.shop_id
where 
  active_seller_status = 1  -- only active sellers 
group by all 

------------------------------------------------------------------
-- WHAT % OF SHOPS HAVE ABOUT VIDEOS?
------------------------------------------------------------------
with shop_stats as (
select 
  sb.shop_id,
  shop_name,
  seller_tier_new, 
  case when state=0 then 1 else 0 end as has_video,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  shop_reviews r
   using (shop_id)
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_about_videos v using (shop_id)
where 
  b.active_seller_status > 0 
group by all 

