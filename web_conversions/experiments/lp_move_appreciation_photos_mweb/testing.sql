------------------------------------------------------------------------------------------------
-- CHECK TO MAKE SURE LISTING VIEW COUNTS ARE THE SAME IN REVIEWS SEEN QUERY VS REGULAR QUERY 
------------------------------------------------------------------------------------------------
select 
  -- lv.listing_id,
  -- count(distinct bl.bucketing_id) as browsers_that_viewed,
  count(lv.sequence_number) as listing_views,
  -- sum(purchased_after_view) as purchases
from
  etsy-bigquery-adhoc-prod._script7472bfed173f9e1e2d8ad0bb22386768877334ae.bucketing_listing bl -- all info from first listing unit was bucketed on 
left join
  etsy-data-warehouse-prod.analytics.listing_views lv
    on bl.bucketing_id=split(lv.visit_id, ".")[0] -- browser ids
    and bl.sequence_number <= lv.sequence_number -- all listing views bucketing moment and after 
where lv._date between date('2025-05-20') and date('2025-05-27') -- dates of the experiment 
and bucketing_id in ('-xIhPmyyJj89g7__D2oK7BINGdMr')
group by all 
-- 5704




  -- begin
-- create or replace temp table test as (
-- select
-- 	date(_partitiontime) as _date,
-- 	split(v.visit_id, ".")[0] as bucketing_id,
-- 	-- visit_id,
--   v.sequence_number,
-- 	beacon.event_name as event_name,
--   coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id 
-- from
-- 	`etsy-visit-pipe-prod.canonical.visit_id_beacons` v
-- inner join 
--   etsy-bigquery-adhoc-prod._script7472bfed173f9e1e2d8ad0bb22386768877334ae.bucketing_listing bl -- only looking at browsers in the experiment 
--     on bl.bucketing_id= split(v.visit_id, ".")[0] -- joining on browser_id
--     and v.sequence_number >= bl.sequence_number -- everything that happens on bucketing moment and after 
-- where
-- 	date(_partitiontime) between date('2025-05-20') and date('2025-05-27') -- dates of the experiment 
-- 	and beacon.event_name in ("listing_page_reviews_seen","view_listing")
-- group by all 
-- ) ;
-- end

select count(case when event_name in ('view_listing') then sequence_number end) as listing_views from etsy-bigquery-adhoc-prod._scriptbce07e692e965f0cb97952a825d655dea77516e2.test where bucketing_id in ('-xIhPmyyJj89g7__D2oK7BINGdMr')
--5704 listing views post bucketing 
select * from etsy-bigquery-adhoc-prod._script7472bfed173f9e1e2d8ad0bb22386768877334ae.bucketing_listing where  bucketing_id in ('-xIhPmyyJj89g7__D2oK7BINGdMr')
------------------------------------------------------------------------------------------------
-- GRAB BROWSERS TO CHECK
------------------------------------------------------------------------------------------------
SELECT 
  _date,
  bucketing_ts, 
  bucketing_id,
  experiment_id,
  associated_ids
from
  `etsy-data-warehouse-prod.catapult_unified.bucketing`
where
  experiment_id = 'growth_regx.lp_move_appreciation_photos_mweb'
  and bucketing_id in ('---GbjIvXd18W7fOW-P-bzTP2bSt','--0_lw25FZTreF0NUWWXjPmF9Rxz','--1C3l9hM574xb2nIHr9H8etAh6A','--3iUkTNv7FvvQ9n01NbsmzbXgmJ','--3wLRtyvMoVzTgy2b4BxdKM8nAs','--3x4n8a5Rl7VQKmVSKw8cMUIt4z','--4SP7noKfPCoHr5Z4jFrSdmF130','--54SOdYFGCTb7Fw93zPqkz5NM_2','--5HgGutKllI_cDMFWZ3CiQ_fmNU','--6YuOi7oUmHOVyVImYLGKtY9lSB')

SELECT 
  _date,
  split(visit_id, ".")[0] as bucketing_id, -- browser_id
  listing_id,
  timestamp_millis(epoch_ms) as listing_ts
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30 -- this will be within time of experiment 
  and split(visit_id, ".")[0]  in ('---GbjIvXd18W7fOW-P-bzTP2bSt','--0_lw25FZTreF0NUWWXjPmF9Rxz','--1C3l9hM574xb2nIHr9H8etAh6A','--3iUkTNv7FvvQ9n01NbsmzbXgmJ','--3wLRtyvMoVzTgy2b4BxdKM8nAs','--3x4n8a5Rl7VQKmVSKw8cMUIt4z','--4SP7noKfPCoHr5Z4jFrSdmF130','--54SOdYFGCTb7Fw93zPqkz5NM_2','--5HgGutKllI_cDMFWZ3CiQ_fmNU','--6YuOi7oUmHOVyVImYLGKtY9lSB')
group by all 

------------------------------------------------------------------------------------------------
-- DISTINCT TIME BETWEEN BUCKTING AND LISTING VIEW
------------------------------------------------------------------------------------------------
with listing_views as (
select
  _date,
  split(visit_id, ".")[0] as bucketing_id, -- browser_id
  listing_id,
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
group by all 
) 
, agg as (
select
  bm.bucketing_id,
  bm.bucketing_ts,
  lv.listing_id,
  lv.listing_ts,
  abs(timestamp_diff(bm.bucketing_ts,lv.listing_ts,second)) as abs_time_between
from 
  bucketing_moment bm
left join  
  listing_views lv 
    using (bucketing_id, _date)
qualify row_number() over (partition by bucketing_id order by abs(timestamp_diff(bm.bucketing_ts,lv.listing_ts,second)) asc) = 1  -- takes listing id closest to bucketing moment
)
select distinct abs_time_between from agg
