with shop_vids as (
select 
  sb.shop_id,
  shop_name,
  case when state=0 then 1 else 0 end as has_video
from 
  etsy-data-warehouse-prod.rollups.seller_basics sb
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_about_videos v using (shop_id)
where 
  sb.active_seller_status > 0 
  and is_frozen = 0 
)
, shop_home_traffic as (
-- total traffic to page 
'shop_home_about_section_seen' -- about section seen
'shop_about_new_video_play' -- about video played 
)
, listing_views as (
select
  shop_id,
  count(distinct listing_id) as listings_viewed,
  count(sequence_number) as listing_views, 
  sum(purchased_after_view) as purchases
from 
  etsy-data-warehouse-prod.analytics.listing_views v 
inner join 
  etsy-data-warehouse-prod.rollups.active_listing_basics b using (listing_id)
)
, purchases as (

)
