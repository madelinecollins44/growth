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

--visits with lp reviews seen event + view listings that have an image in review and convert
with desktop_visits as (
select 
  visit_id
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
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` using (visit_id)
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
, lv_converts as (
select
  lv.	visit_id,
	lv.listing_id,
  lv.sequence_number,
  c.purchased_after_view -- was that listing purchased after view
from 
  listing_views lv
left join 
  etsy-data-warehouse-prod.analytics.listing_views c 
    on lv.visit_id=c.visit_id
    and lv.sequence_number=c.sequence_number
    and lv.listing_id=cast(c.listing_id as string)
where c._date >= current_date-30
and lv.saw_reviews > 0 -- only visits that have seen reviews 
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
  count(distinct lv.visit_id) as visits_saw_reviews,
  count(distinct case when purchased_after_view > 0 then visit_id end) as visits_saw_reviews_and_purchased,
from 
  lv_converts lv
inner join 
	reviews r
		on lv.listing_id=cast(r.listing_id as string)
where r.has_image > 0
