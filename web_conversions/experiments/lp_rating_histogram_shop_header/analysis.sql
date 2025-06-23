/* 
begin
create or replace temp table bucketing_listing as (
with listing_views as ( -- get all listing views that happened during time of experiment 
select
  _date,
  visit_id,
  split(visit_id, ".")[0] as bucketing_id, -- browser_id
  listing_id,
  sequence_number,
  added_to_cart,
  purchased_after_view,
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
  variant_id,
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

with listing_events as ( -- gets listing_id for all clicks on review signals in buy box + listing views 
select
	date(_partitiontime) as _date,
	v.visit_id,
  split(visit_id, ".")[0] as bucketing_id,
  variant_id,
  -- v.sequence_number,
  case 
    when v.visit_id = bl.visit_id and v.sequence_number >= bl.sequence_number then 1 -- if within the same visit AND on bucketing sequence number or after 
    when v.visit_id > bl.visit_id then 1 -- after the bucketing visit_id
  end as after_bucketing_flag,
	beacon.event_name as event_name,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id,
  count(sequence_number) as actions, 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` v
inner join 
  etsy-bigquery-adhoc-prod._scriptdee81035175fba82e21a15db89d2ad31b2dc12b4.bucketing_listing bl -- only looking at browsers in the experiment 
    on bl.bucketing_id= split(v.visit_id, ".")[0] -- joining on browser_id
    and v.visit_id >= bl.visit_id -- everything that happens on bucketing moment and after (cant do sequence number bc there is only one)
where
	date(_partitiontime) between date('2025-05-20') and date('2025-05-27') -- dates of the experiment 
	and beacon.event_name in ('reviews_anchor_click','view_listing')
group by all 
)
, listing_stats as (
select
  _date,
  visit_id,
  bucketing_id,
  listing_id,
  count(v.sequence_number) as views,
  sum(case when event_type in ('view_listing') then actions end) as listing_views,
  sum(case when event_type in ('reviews_anchor_click') then actions end) as review_anchor_clicks,
  sum(added_to_cart) as atc,
  sum(purchased_after_view) as purchase,
from 
  listing_events e
inner join
  etsy-data-warehouse-prod.analytics.listing_views v
    on e.visit_id= v.visit_id
    and e.listing_id= v.listing_id
where 
  _date >= current_date-14 -- this will be within time of experiment 
)
