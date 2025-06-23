/* begin
create or replace temp table bucketing_listing as (
with listing_views as ( -- get all listing views that happened during time of experiment 
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
  _date >= current_date-14 -- this will be within time of experiment 
)
, bucketing_moment as ( -- pulls out bucketing moment of each browser 
select 
  variant_id,
  min(_date) as _date,
  min(bucketing_ts) as bucketing_ts,  -- first bucketing moment, this is what will be used to join time 
  bucketing_id,
  experiment_id,
from
  `etsy-data-warehouse-prod.catapult_unified.bucketing`
where
  experiment_id = 'growth_regx.lp_rating_histogram_shop_header_desktop'
  -- and variant_id in ('on')
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
*/ 

with listing_events as ( -- get all view listing events that happen after bucketing moment 
select
	_date,
	v.visit_id,
  v.sequence_number,
  case 
    when v.visit_id = bl.visit_id and v.sequence_number >= bl.sequence_number then 1 -- if within the same visit AND on bucketing sequence number or after 
    when v.visit_id > bl.visit_id then 1 -- after the bucketing visit_id
  end as after_bucketing_flag,
	event_type,
  listing_id,
from
	etsy-data-warehouse-prod.analytics.listing_views v
inner join 
  etsy-bigquery-adhoc-prod._scriptdee81035175fba82e21a15db89d2ad31b2dc12b4.bucketing_listing bl -- only looking at browsers in the experiment 
    on bl.bucketing_id= split(v.visit_id, ".")[0] -- joining on browser_id
    and v.visit_id >= bl.visit_id -- everything that happens on bucketing moment and after (cant do sequence number bc there is only one)
where
	_date between date('2025-05-20') and date('2025-05-27') -- dates of the experiment 
	and event_type in ("view_listing")
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
