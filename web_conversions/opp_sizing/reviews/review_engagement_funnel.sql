/* PURPOSE: this funnel is meant to help us understand if there is a correlation between review engagement and conversion, and if this track of work is worth continuing. */

----- total visits in the last 30 days
select 
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_traffic,
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_traffic,
from 
  etsy-data-warehouse-prod.weblog.visits 
where 
 _date >= current_date-30
 group by all 
  
/* listing views */
select
  count(distinct visit_id) as visits_w_lv,
  count(visit_id) as listing_views,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_visits_w_lv,
  count(case when platform in ('desktop') then visit_id end) as desktop_listing_views,
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_visits_w_lv,
  count(case when platform in ('mobile_web') then visit_id end) as mweb_listing_views,
from 
  etsy-data-warehouse-prod.analytics.listing_views
where
  _date >= current_date-30


----- % of lv with review attributes
with listing_views as (
select
  platform,
  listing_id,
  visit_id,
  count(visit_id) as listing_views
from 
  etsy-data-warehouse-prod.analytics.listing_views
where
  _date >= current_date-30
  and platform in ('desktop','mobile_web','boe')
group by all
)
, reviews as (
select
  listing_id,
  sum(has_review) as has_review,
  sum(has_image) as has_image,
  sum(has_video) as has_video,
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews  
group by all
)
select
  platform,
  sum(lv.listing_views) as total_listing_views,
  count(distinct lv.visit_id) as visits_w_listing_view,
  count(distinct case when r.has_review > 0 then lv.listing_id end) listings_w_review,
  count(distinct case when r.has_image > 0 then lv.listing_id end) listings_w_image,
  count(distinct case when r.has_video > 0 then lv.listing_id end) listings_w_video,
  sum(case when r.has_review > 0 then listing_views end) has_review_lv,
  sum(case when r.has_image > 0 then listing_views end) has_image_lv,
  sum(case when r.has_video > 0 then listing_views end) has_video_lv,
  count(distinct case when r.has_review > 0 then lv.visit_id end) visits_w_review,
  count(distinct case when r.has_image > 0 then lv.visit_id end) visits_w_image,
  count(distinct case when r.has_video > 0 then lv.visit_id end) visits_w_video,

from
  listing_views lv
left join 
  reviews r using (listing_id)
group by all

----- review engagement post listing view
with desktop_visits as (
select 
  platform,
  visit_id,
  converted
from 
  etsy-data-warehouse-prod.weblog.visits 
where 
  platform in ('desktop','mobile_web','boe') 
  and _date >= current_date-30
)
, listing_events as (
select
  platform,
	date(_partitiontime) as _date,
	visit_id,
  converted,
  sequence_number,
	case 
    when beacon.event_name in ('view_listing') then 'view_listing'
    else 'review_engagement' 
  end as event_name,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id 
from
  desktop_visits 
inner join 
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` using (visit_id) -- only looking at desktop visits 
where
	date(_partitiontime) >= current_date-30
	and beacon.event_name in ("sort_reviews","listing_page_reviews_pagination","appreciation_photo_overlay_opened","view_listing")
group by all 
)
, ordered_events as (
select
  platform,
	_date,
	visit_id,
  converted,
	listing_id,
  sequence_number,
	event_name,
	lead(event_name) over (partition by visit_id, listing_id order by sequence_number) as next_event,
	lead(sequence_number) over (partition by visit_id, listing_id order by sequence_number) as next_sequence_number
from 
	listing_events
)
, listing_views as (
select
	visit_id,
  platform,
  converted,
	listing_id,
  sequence_number,
	case when next_event in ('review_engagement') then 1 else 0 end as engaged_w_reviews
from
	ordered_events
where 
	event_name in ('view_listing')
)
select
  platform,
  count(visit_id) as listing_views,
  count(case when engaged_w_reviews > 0 then visit_id end) as saw_listing_and_reviews,
  count(distinct visit_id) as visits_w_lv,
  count(distinct case when engaged_w_reviews > 0 then visit_id end) as visits_w_listing_and_reviews,
  count(distinct case when converted > 0 then visit_id end) as visits_w_lv_and_convert,
  count(distinct case when converted> 0 and engaged_w_reviews > 0 then visit_id end) as visits_w_listing_and_reviews_and_convert,
from 
  listing_views
group by all 


------ overall counts to confirm 
select
  platform,
  event_type,
  count(distinct visit_id) as unique_visits,
  count(visit_id) as events
from 
  etsy-data-warehouse-prod.weblog.events e
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date-30	
  and event_type in ("sort_reviews","listing_page_reviews_pagination","listing_page_reviews_container_top_seen","appreciation_photo_overlay_opened","view_listing","listing_page_reviews_seen")
  and platform in ('desktop','mobile_web','boe') 
group by all 
order by 1,2 desc
