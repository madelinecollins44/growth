
-- CREATE TABLE TO GET REVIEWS ACROSS ALL LISTINGS
create or replace table etsy-data-warehouse-dev.madelinecollins.holder_table as ( -- looks across all purchased listings, not just viewed listings
with listings_agg as (
select
  listing_id,
  coalesce(count(distinct case when has_review > 0 then transaction_id end),0) as reviews,
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
group by all 
)
select
  listing_id,
  case
    when reviews = 0 then '0 reviews'
    when reviews >= 1 and reviews < 10 then '1- 9'
    when reviews >= 10 and reviews < 30 then '10-29'
    when reviews >= 30 and reviews < 50 then '30-50'
    when reviews >= 50 and reviews < 100 then '50-99'
    when reviews >= 100 then '100+'
    else 'error'
  end as review_count,
from 
 listings_agg
group by all 
);


-- CREATE TEMP TABLE TO GET ALL BROWSERS, VARIANTS BUCKETED IN EXPERIMENT
create or replace table etsy-data-warehouse-dev.madelinecollins.bucketing_listing as (
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
  _date between date('2025-06-13') and date('2025-06-22')  -- this will be within time of experiment 
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


-- CREATE TEMP TABLE TO GET ALL BEACONS EVENTS 
create or replace table etsy-data-warehouse-dev.madelinecollins.beacons_events as (
select
	v.browser_id,
  variant_id,
  event_name, 
  event_timestamp,
  coalesce((regexp_extract(loc, r'listing/(\d+)')),(regexp_extract(loc, r'cart/(\d+)'))) as listing_id
from
	etsy-visit-pipe-prod.canonical.beacon_main_2025_06 v
inner join 
  etsy-data-warehouse-dev.madelinecollins.bucketing_listing bl -- only looking at browsers in the experiment 
    on bl.bucketing_id= v.browser_id -- joining on browser_id
    -- and v.event_timestamp >= cast(unix_seconds(bucketing_ts) as int64)-- everything after bucketing moment 
where
	_date between date('2025-06-13') and date('2025-06-22') -- dates of the experiment 
	and event_name in ('reviews_anchor_click','view_listing','listing_page_reviews_seen')
group by all 
);


-- PUT IT TOGETHER
-- PUT IT TOGETHER
with listing_events as ( -- get listing_id for all clicks on review signals in buy box + listing views 
select
	browser_id,
  variant_id,
  listing_id,
  sum(case when event_name in ('view_listing') then 1 else 0 end) as listing_views, 
  sum(case when event_name in ('reviews_anchor_click') then  1 else 0 end) as review_clicks,   
  sum(case when event_name in ('checkout_start') then  1 else 0 end) as checkout_starts, 
  sum(case when event_name in ('listing_page_reviews_seen') then  1 else 0 end) as reviews_seen,
from
	etsy-data-warehouse-dev.madelinecollins.beacons_events 
group by all 
)
, listing_stats as (
select 
  variant_id,
  browser_id,
  v.listing_id,
  count(v.sequence_number) as views,
  sum(views) as listing_views, 
  sum(added_to_cart) as atc,
  sum(purchased_after_view) as purchase,
  avg(coalesce(v.price_usd, l.price_usd/100)) as avg_price_usd
from 
  etsy-data-warehouse-prod.analytics.listing_views  v
inner join
  (select
      browser_id,
      listing_id,
      variant_id,
      count(*) as views,
    from
      etsy-data-warehouse-dev.madelinecollins.beacons_events 
    where event_name in ('view_listing')
    group by 1,2,3) e
  on e.browser_id=split(v.visit_id, ".")[0] 
  -- and e.event_timestamp=v.epoch_ms -- matching timestamps
  and e.listing_id= cast(v.listing_id as string)
inner join 
  etsy-data-warehouse-prod.listing_mart.listings l    
    on v.listing_id=l.listing_id
where 
  _date between date('2025-06-13') and date('2025-06-22')  -- this will be within time of experiment
group by all  
)
, agg_listing_stats as (
select 
  e.variant_id,
  e.listing_id,
  -- count(distinct bucketing_id) as browsers, 
  sum(e.listing_views) as listing_views, 
  sum(review_clicks) as review_clicks,   
  sum(checkout_starts) as checkout_starts,
  sum(reviews_seen) as reviews_seen,
  sum(views) as views,
  sum(atc) as atc,
  sum(purchase) as purchase,
  avg(avg_price_usd) as avg_price_usd
from 
  listing_events e
inner join 
  listing_stats s 
    on e.variant_id=s.variant_id
    and e.browser_id=s.browser_id 
    and e.listing_id=cast(s.listing_id as string)
group by all
)
select
  variant_id,
  coalesce(review_count, 'no transactions') as rating_status,
  sum(listing_views) as listing_views, 
  sum(review_clicks) as review_clicks,   
  sum(checkout_starts) as checkout_starts,
  sum(views) as views,
  sum(atc) as atc,
  sum(purchase) as purchase,
  sum(reviews_seen) as reviews_seen,
  avg(avg_price_usd) as avg_price_usd
from 
  agg_listing_stats s
left join 
  etsy-data-warehouse-dev.madelinecollins.holder_table r
    on s.listing_id=cast(r.listing_id as string) 
group by all 
order by 1,2 desc
