begin
create or replace temp table bucketing_listing as (
with listing_views as (
select
  _date,
  visit_id,
  split(visit_id, ".")[0] as bucketing_id, -- browser_id
  listing_id,
  sequence_number,
  timestamp_millis(epoch_ms) as listing_ts
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30 -- this will be within time of experiment 
)
, bucketing_moment as ( -- pulls out bucketing moment 
select 
  min(_date) as _date,
  min(bucketing_ts) as bucketing_ts,  -- first bucketing moment, this is what will be used to join time 
  bucketing_id,
  experiment_id,
from
  `etsy-data-warehouse-prod.catapult_unified.bucketing`
where
  experiment_id = 'growth_regx.lp_move_appreciation_photos_mweb'
  and variant_id in ('on')
group by all 
) 
select
  bm.bucketing_id,
  bm.bucketing_ts,
  lv.listing_id,
  visit_id,
  lv.sequence_number,
  lv.listing_ts,
  abs(timestamp_diff(bm.bucketing_ts,lv.listing_ts,second)) as abs_time_between
from 
  bucketing_moment bm
left join  
  listing_views lv 
    using (bucketing_id, _date)
qualify row_number() over (partition by bucketing_id order by abs(timestamp_diff(bm.bucketing_ts,lv.listing_ts,second)) asc) = 1  -- takes listing id closest to bucketing moment
);
end
-- select count(distinct bucketing_id) from etsy-bigquery-adhoc-prod._scriptdee81035175fba82e21a15db89d2ad31b2dc12b4.bucketing_listing

with listing_events as (
select
	date(_partitiontime) as _date,
	v.visit_id,
  v.sequence_number,
  case 
    when v.visit_id = bl.visit_id and v.sequence_number >= bl.sequence_number then 1 -- if within the same visit AND on bucketing sequence number or after 
    when v.visit_id > bl.visit_id then 1 -- after the bucketing visit_id
  end as after_bucketing_flag,
	beacon.event_name as event_name,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` v
inner join 
  etsy-bigquery-adhoc-prod._scriptdee81035175fba82e21a15db89d2ad31b2dc12b4.bucketing_listing bl -- only looking at browsers in the experiment 
    on bl.bucketing_id= split(v.visit_id, ".")[0] -- joining on browser_id
    and v.visit_id >= bl.visit_id -- everything that happens on bucketing moment and after (cant do sequence number bc there is only one)
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
where after_bucketing_flag = 1 -- only looking at post bucketing 
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
