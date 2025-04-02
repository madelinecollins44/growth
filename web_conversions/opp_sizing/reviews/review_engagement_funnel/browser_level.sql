with desktop_visits as (
select 
  platform,
  visit_id,
  browser_id,
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
  browser_id,
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
  browser_id,
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
  browser_id,
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
  -- visit level metrics
  count(visit_id) as listing_views,
  count(case when engaged_w_reviews > 0 then visit_id end) as saw_listing_and_engage,
  count(distinct visit_id) as visits_w_lv,
  count(distinct case when engaged_w_reviews > 0 then visit_id end) as visits_w_listing_and_engage,
  count(distinct case when converted > 0 then visit_id end) as visits_w_lv_and_convert,
  count(distinct case when converted> 0 and engaged_w_reviews > 0 then visit_id end) as visits_w_listing_and_engage_and_convert,
  -- browser level metrics
  count(distinct browser_id) as browser_w_lv,
  count(case when engaged_w_reviews > 0 then browser_id end) as browser_w_lv_and_engage,
  count(distinct case when converted > 0 then browser_id end) as browsers_w_lv_and_convert,
  count(distinct case when converted > 0 and engaged_w_reviews > 0 then browser_id end) as browsers_w_listing_and_engage_and_convert,
from 
  listing_views
group by all 
