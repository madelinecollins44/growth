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
)
, purchases as (

)
