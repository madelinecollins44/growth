-- CREATE TABLE TO GET RATINGS ACROSS ALL LISTINGS
create or replace table etsy-data-warehouse-dev.madelinecollins.listings_by_ratings as ( -- looks across all purchased listings, not just viewed listings
with listings_agg as (
select
  listing_id,
  coalesce(count(distinct case when has_review > 0 then transaction_id end),0) as reviews,
  coalesce(count(distinct case when date(transaction_date) >= current_date-365 and has_review > 0 then transaction_id end),0) as reviews_in_last_year,
  coalesce(count(distinct case when date(transaction_date) < current_date-365 and has_review > 0 then transaction_id end),0) as  reviews_in_before_last_year,
  coalesce(round(avg(case when date(transaction_date) >= current_date-365 then rating end),1),0) as avg_rating_in_last_year,
  coalesce(round(avg(case when date(transaction_date) < current_date-365  then rating end),1),0) as avg_rating_in_before_last_year,
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
group by all 
)
select
  case
    when reviews = 0 then '0 reviews'
    when reviews_in_last_year = 0 and reviews_in_before_last_year > 0 then '0 rating in past year but has reviews'
    when round(avg_rating_in_last_year) = 5 then 'avg 5 stars rating'
    when round(avg_rating_in_last_year) = 4 then 'avg 4 stars rating'
    when round(avg_rating_in_last_year) = 3 then 'avg 3 stars rating'
    when round(avg_rating_in_last_year) = 2 then 'avg 2 stars rating'
    when round(avg_rating_in_last_year) = 1 then 'avg 1 stars rating'
    else 'error'
  end as rating_status,
 listing_id
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
	v.visit_id,
  split(v.visit_id, ".")[0] as bucketing_id,
  variant_id,
  coalesce(case 
    when v.visit_id = bl.visit_id and v.sequence_number >= bl.sequence_number then 1 -- if within the same visit AND on bucketing sequence number or after 
    when v.visit_id > bl.visit_id then 1 -- after the bucketing visit_id
  end,0) as after_bucketing_flag,
  beacon.event_name,
  v.sequence_number,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), -- view_listing
            (regexp_extract(beacon.loc, r'listing/(\d+)')), -- reviews_anchor_click
            (split((select value from unnest(beacon.properties.key_value) where key = "listing_ids"), ',')[offset(0)])) -- checkout_start 
  as listing_id,
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` v
inner join 
  etsy-data-warehouse-dev.madelinecollins.bucketing_listing bl -- only looking at browsers in the experiment 
    on bl.bucketing_id= split(v.visit_id, ".")[0] -- joining on browser_id
    and v.visit_id >= bl.visit_id -- everything that happens on bucketing moment and after (cant do sequence number bc there is only one)
where
	date(_partitiontime) between date('2025-06-13') and date('2025-06-22') -- dates of the experiment 
	and beacon.event_name in ('reviews_anchor_click','view_listing','checkout_start')
group by all 
);

-- PUT IT ALL TOGETHER
with listing_events as ( -- get listing_id for all clicks on review signals in buy box + listing views 
select
	visit_id,
  split(visit_id, ".")[0] as bucketing_id,
  variant_id,
  listing_id,
  count(case when event_name in ('view_listing') then sequence_number end) as listing_views, 
  count(case when event_name in ('reviews_anchor_click') then sequence_number end) as review_clicks,   
  count(case when event_name in ('checkout_start') then sequence_number end) as checkout_starts, 
  count(case when event_name in ('listing_page_reviews_seen') then sequence_number end) as reviews_seen, 
from
	etsy-data-warehouse-dev.madelinecollins.beacons_events 
where 
  after_bucketing_flag > 0 -- only looks at things after bucketing moment 
group by all 
)
, listing_stats as (
select 
  variant_id,
  e.visit_id,
  bucketing_id,
  v.listing_id,
  count(v.sequence_number) as views,
  count(case when event_name in ('view_listing') then e.sequence_number end) as listing_views, 
  sum(added_to_cart) as atc,
  sum(purchased_after_view) as purchase,
  avg(coalesce(v.price_usd, l.price_usd/100)) as avg_price_usd
from 
  etsy-data-warehouse-prod.analytics.listing_views  v
inner join
  etsy-data-warehouse-dev.madelinecollins.beacons_events  e
    on e.visit_id =  v.visit_id
    and e.sequence_number =  v.sequence_number
    and e.listing_id= cast(v.listing_id as string)
    and event_name in ('view_listing')
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
    and e.visit_id=s.visit_id
    and e.bucketing_id=s.bucketing_id 
    and e.listing_id=cast(s.listing_id as string)
group by all
)
select
  variant_id,
  coalesce(rating_status, 'no transaction') as rating_status,
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
  etsy-data-warehouse-dev.madelinecollins.listings_by_ratings r
    on s.listing_id=cast(r.listing_id as string) 
group by all 
order by 1,2 desc
