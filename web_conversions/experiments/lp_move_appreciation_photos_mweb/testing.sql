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
