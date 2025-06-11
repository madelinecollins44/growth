-- adding in platform
with listing_events as (
select
	date(_partitiontime) as _date,
	visit_id,
  v.sequence_number,
	beacon.event_name as event_name,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` v
inner join 
  etsy-bigquery-adhoc-prod._script7472bfed173f9e1e2d8ad0bb22386768877334ae.bucketing_listing bl -- only looking at browsers in the experiment 
    on bl.bucketing_id= split(v.visit_id, ".")[0] -- joining on browser_id
    and v.sequence_number >= bl.sequence_number -- everything that happens on bucketing moment and after 
where
	date(_partitiontime) between date('2025-05-20') and date('2025-05-27') -- dates of the experiment 
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
, lv_stats as (
select
	lv.listing_id,
	case when a.listing_id is null then 1 else 0 end as missing_in_analytics,
	count(lv.visit_id) as listing_views,
	count(a.visit_id) as a_listing_views,
	count(case when saw_reviews = 1 then lv.visit_id end) as views_and_reviews_seen,
	sum(case when saw_reviews = 1 then purchased_after_view end) as saw_reviews_and_purchased,
	sum(purchased_after_view) as purchases
from 
	listing_views lv
left join 
	etsy-data-warehouse-prod.analytics.listing_views a
		on lv.listing_id=cast(a.listing_id as string)
		and lv.visit_id=a.visit_id
		and lv.sequence_number=a.sequence_number	
where a._date >=current_date-30
group by all
)
, photo_reviews as (
select
  listing_id,
  sum(has_image) as images
from etsy-data-warehouse-prod.rollups.transaction_reviews
where has_review > 0
group by all 
order by 2 desc
)
select
  case
    when images = 0 then '0'
    when images = 1 then '1'
    when images = 2 then '2'
    when images = 3 then '3'
    when images = 4 then '4'
    else '5+' 
  end as review_photos,
  sum(missing_in_analytics) as errors,
  count(distinct rv.listing_id) as listings_viewed,
  sum(rv.listing_views) as listing_views,
  sum(rv.views_and_reviews_seen) as views_and_reviews_seen,
  sum(rv.purchases) as purchases,
  sum(rv.saw_reviews_and_purchased) as saw_reviews_and_purchased,
from
  lv_stats rv
left join 
  photo_reviews n 
		on rv.listing_id = cast(n.listing_id as string)
group by all
