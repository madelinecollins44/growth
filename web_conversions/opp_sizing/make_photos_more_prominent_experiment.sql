--------------
--OPP SIZING
--------------
-- total traffic counts
select
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_visits,  
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_visits
from 
  etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30

-- visits with lp review seen event 
select
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_visits,  
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_visits
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 
  v._date >= current_date-30
  and e.event_type in ('listing_page_reviews_seen')

--visits with lp reviews seen event + view listings that have an image in review
with desktop_visits as (
select 
  distinct visit_id 
from 
  etsy-data-warehouse-prod.weblog.visits 
where 
  platform in ('mobile_web') 
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
  count(distinct visit_id) as visits_view_listings,
  count(distinct case when saw_reviews > 0 then visit_id end) as visits_view_listing_and_reviews 
from 
  listing_views lv
inner join 
	reviews r
		on lv.listing_id=cast(r.listing_id as string)
where r.has_image > 0
