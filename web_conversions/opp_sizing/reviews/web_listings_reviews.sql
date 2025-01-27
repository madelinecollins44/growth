--total desktop visits in the last 30 days
select 
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_traffic,
  count(distinct case when user_id is not null and platform in ('desktop') then visit_id end) as signedin_desktop_traffic,
  count(distinct case when user_id is null and platform in ('desktop') then visit_id end) as signedout_desktop_traffic,
from 
  etsy-data-warehouse-prod.weblog.visits 
where 
 _date >= current_date-30
 and platform in ('desktop')
 group by all 

  
--desktop listing views
select
  count(distinct visit_id) as visits_w_lv,
  count(visit_id) as listing_views
from 
  etsy-data-warehouse-prod.analytics.listing_views
where
  _date >= current_date-30
  and platform in ('desktop')

-- % of lv with review attributes
with listing_views as (
select
  listing_id,
  count(visit_id) as listing_views
from 
  etsy-data-warehouse-prod.analytics.listing_views
where
  _date >= current_date-30
  and platform in ('desktop')
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
  sum(lv.listing_views) as total_listing_views,
  count(distinct case when r.has_review > 0 then lv.listing_id end) listings_w_review,
  count(distinct case when r.has_image > 0 then lv.listing_id end) listings_w_image,
  count(distinct case when r.has_video > 0 then lv.listing_id end) listings_w_video,
  sum(case when r.has_review > 0 then listing_views end) has_review_lv,
  sum(case when r.has_image > 0 then listing_views end) has_image_lv,
  sum(case when r.has_video > 0 then listing_views end) has_video_lv,
from
  listing_views lv
left join 
  reviews r using (listing_id)

--listing views that also saw reviews
with desktop_visits as (
select 
  distinct visit_id 
from 
  etsy-data-warehouse-prod.weblog.visits 
where 
  platform in ('desktop') 
  and _date >= current_date-30
)
, listing_events as (
select
	date(_partitiontime) as _date,
	visit_id,
  sequence_number,
	beacon.event_name as event_name,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id 
from
  desktop_visits 
inner join 
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` using (visit_id) -- only looking at desktop visits 
where
	date(_partitiontime) >= current_date-30
	and beacon.event_name in ("listing_page_reviews_seen","view_listing")
group by all 
)
, ordered_events as (
select
	_date,
	visit_id,
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
	listing_id,
  sequence_number,
	case when next_event in ('listing_page_reviews_seen') then 1 else 0 end as saw_reviews
from
	ordered_events
where 
	event_name in ('view_listing')
)
select
  count(visit_id) as listing_views,
  count(case when saw_reviews > 0 then visit_id end) as saw_listing_and_reviews
from 
  listing_views


--listing views of listings with a review + sees the review section
with desktop_visits as (
select 
  distinct visit_id 
from 
  etsy-data-warehouse-prod.weblog.visits 
where 
  platform in ('desktop') 
  and _date >= current_date-30
)
, listing_events as (
select
	date(_partitiontime) as _date,
	visit_id,
  sequence_number,
	beacon.event_name as event_name,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id 
from
  desktop_visits 
inner join 
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` using (visit_id) -- only looking at desktop visits 
where
	date(_partitiontime) >= current_date-30
	and beacon.event_name in ("listing_page_reviews_seen","view_listing")
group by all 
)
, ordered_events as (
select
	_date,
	visit_id,
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
	listing_id,
  sequence_number,
	case when next_event in ('listing_page_reviews_seen') then 1 else 0 end as saw_reviews
from
	ordered_events
where 
	event_name in ('view_listing')
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
  count(visit_id) as listing_views,
  count(case when saw_reviews > 0 then visit_id end) as saw_listing_and_reviews
from 
  listing_views lv
inner join 
	reviews r
		on lv.listing_id=cast(r.listing_id as string)
where r.has_review > 0
