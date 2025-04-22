with listing_events as (
select
	date(_partitiontime) as _date,
	visit_id,
  platform,
  sequence_number,
	beacon.event_name as event_name,
  case when beacon.loc like ('%external=1%') then 1 else 0 end as external_view,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
inner join 
  etsy-data-warehouse-prod.weblog.visits using (visit_id)
where
	date(_partitiontime) >= current_date-30
  and _date >= current_date-30
	and beacon.event_name in ("listing_page_reviews_seen","view_listing")
  and platform in ('mobile_web','desktop')
	-- and beacon.event_source in ('web')
group by all 
)
, ordered_events as (
select
	_date,
	visit_id,
  platform,
	listing_id,
  sequence_number,
	event_name,
  external_view,
	lead(event_name) over (partition by visit_id, listing_id order by sequence_number) as next_event,
	lead(sequence_number) over (partition by visit_id, listing_id order by sequence_number) as next_sequence_number
from 
	listing_events
)
, listing_views as (
select
	visit_id,
  platform,
	listing_id,
  sequence_number,
  external_view,
	case when next_event in ('listing_page_reviews_seen') then 1 else 0 end as saw_reviews
from
	ordered_events
where 
	event_name in ('view_listing')
)
, lv_stats as (
select
  lv.platform,
	lv.listing_id,
  lv.external_view,
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
left join 
  etsy-data-warehouse-prod.listing_mart.listings p 
    on cast(p.listing_id as string)=lv.listing_id
where a._date >=current_date-30
group by all
)
select
  platform,
  external_view,
	sum(missing_in_analytics) as errors,
  count(distinct rv.listing_id) as listings_viewed,
  sum(rv.listing_views) as listing_views,
  sum(rv.purchases) as purchases,
  sum(rv.saw_reviews_and_purchased) as saw_reviews_and_purchased,
  --listing page 
  sum(rv.views_and_reviews_seen) as views_and_reviews_seen,
from
  lv_stats rv
group by all


------------------------------------------------------------
-- TESTING
------------------------------------------------------------
-- TEST 1: make sure listing views match up from analytics 
select 
  case when url like ('%external=1%') then 1 else 0 end as external_listing,
  count(lv.sequence_number) as lv, 
  sum(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.weblog.events e
inner join 
  etsy-data-warehouse-prod.analytics.listing_views lv 
    on e.visit_id=lv.visit_id
    and e.sequence_number=lv.sequence_number
    and e.listing_id= cast(lv.listing_id as string)
where 
  event_type in ('view_listing') 
  and lv._date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all
/*external_listing	lv	purchases
0	1089720954	21960013
1	54208698	501330
	*/
	
select 
  count(lv.sequence_number) as lv, 
  sum(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.analytics.listing_views lv 
where 
  lv._date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all
/* lv	purchases
1143929652	22461343
	*/
