begin
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
