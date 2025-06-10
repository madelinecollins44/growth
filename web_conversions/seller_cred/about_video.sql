with shop_stats as (
select 
  sb.shop_id,
  shop_name,
  seller_tier_new, 
  case when state=0 then 1 else 0 end as has_video,
  sum(total_gms) as total_gms,
  sum(total_orders) as total_orders,
  sum(total_quantity_sold) as total_quantity_sold
from 
  etsy-data-warehouse-prod.rollups.seller_basics sb
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_about_videos v using (shop_id)
where 
  sb.active_seller_status > 0 
  and is_frozen = 0 
group by all 
)
, shop_home_traffic as (
select
  (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id,
  -- visits
  count(distinct case when beacon.event_name in ('shop_home') then visit_id end) as shop_home_visits, 
  count(distinct case when beacon.event_name in ('shop_home_about_section_seen') then visit_id end) as section_seen_visits, 
  count(distinct case when beacon.event_name in ('shop_about_new_video_play') then visit_id end) as video_play_visits, 
  -- pageviews
  count(case when beacon.event_name in ('shop_home') then visit_id end) as shop_home_views, 
  count(case when beacon.event_name in ('shop_home_about_section_seen') then visit_id end) as section_seen_views, 
  count(case when beacon.event_name in ('shop_about_new_video_play') then visit_id end) as video_play_views, 
from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
	date(_partitiontime) >= current_date-14
  and (beacon.event_name in ('shop_home','shop_home_about_section_seen','shop_about_new_video_play'))
  and (beacon.event_source in ('web'))
group by all
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
where
  _date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all 
)
, reviews as (
select
  shop_id,
  sum(has_review) as total_reviews
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
group by all 
)
select
  seller_tier_new, 
  has_video,
  case when r.shop_id is not null then 1 else 0 end as has_reviews,
  count(distinct s.shop_id) as active_shops,
  sum(total_gms) as total_gms,
  sum(total_orders) as total_orders,
  sum(total_quantity_sold) as total_quantity_sold,
-- traffic
  sum(shop_home_views) as shop_home_views, 
  sum(section_seen_views)as section_seen_views, 
  sum(video_play_views) as video_play_views, 
-- lv
  sum(listings_viewed) as listings_viewed,
  sum(listing_views) as listing_views, 
  sum(purchases) as purchases
from 
  shop_stats s
left join 
  shop_home_traffic t on cast(s.shop_id as string)=t.shop_id
left join  
  listing_views lv on s.shop_id=lv.shop_id
left join 
  reviews r on s.shop_id=r.shop_id
group by all 
