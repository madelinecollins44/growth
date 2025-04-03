-- get browser + listing level stats (browser is platform specific)
with lv_stats as (
select 
  platform,
  split(visit_id,'.')[safe_offset(0)] as browser_id, -- browser is specific to platform, so no need to look on visit level 
  listing_id,
  count(visit_id) as listing_views,
  count(distinct visit_id) as unique_visits,
  sum(purchased_after_view) as purchases
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  platform in ('desktop','mobile_web')
  and _date <= current_date-30
group by all 
)
-- get engagements for each browser on each listing
, review_engagements as (
select
  beacon.browser_id,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id,
  count(sequence_number) as engagements,
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` b 
inner join -- join here to get platform and only look at browsers that have viewed a listing 
  lv_stats s
    on s.browser_id= b.beacon.browser_id
where
	date(_partitiontime) <= current_date-30
	and (beacon.event_name in ("listing_page_reviews_pagination","appreciation_photo_overlay_opened") --all these events are lp specific 
      or (beacon.event_name) in ("sort_reviews") and (select value from unnest(beacon.properties.key_value) where key = "primary_event_source") in ('view_listing'))  -- sorting on listing page 
group by all 
)
-- find engagement on browser/ listing level
select
  s.platform,
--lv stats
  count(distinct s.browser_id) as browsers_w_lv,
  count(distinct case when purchases > 0 then s.browser_id end) as browsers_w_purchase,
  -- sum(unique_visits) as visits_w_lv,
  -- sum(case when purchases > 0 then unique_visits end) as visits_w_purchase,
  sum(listing_views) as total_lv,
  sum(purchases) as total_purchases,
-- engagement stats
  count(distinct r.browser_id) as browsers_w_engagement,
  count(distinct case when purchases > 0 then r.browser_id end) as engaged_browsers_w_purchase,
  sum(engagements) as total_engagements,
from 
  lv_stats s
left join 
  review_engagements r
    on s.browser_id=r.browser_id
    and cast(s.listing_id as string)=r.listing_id
group by all 

-----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- TESTING 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
/* TEMP TABLES:
etsy-bigquery-adhoc-prod._scriptff50f4cf1cc12b75b545c2c66abca1b5c8d2e056.lv_stats : 30 DAY LV STATS
etsy-bigquery-adhoc-prod._script9b1b1d2b53f289cb4f1e48f56f21e0abc32b9288.lv_stats_3: 3 DAY LV STATS

-- TEST 1: make sure browser + listing counts are accurate
with lv_stats as (
select 
  platform,
  visit_id,
  split(visit_id,'.')[safe_offset(0)] as browser_id, -- browser is specific to platform, so no need to look on visit level 
  listing_id,
  count(visit_id) as listing_views,
  count(distinct visit_id) as unique_visits,
  sum(purchased_after_view) as purchases
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  platform in ('desktop','mobile_web')
  and _date >= current_date-3
group by all 
)
-- select browser_id, count(distinct visit_id), count(distinct listing_id) from lv_stats  group by all having count(distinct listing_id) < 50 and  count(distinct visit_id) < 50 order by 2 desc limit 5
-- browser_id	f0_	f1_
-- SLrH5X9gn_dAdDB4LxUblGuf07wh	49	26
-- jy2FyYjxCI68KxvAQyXRM-KWQo_B	49	36
-- I3Ey5uu9Un-X3Fxa020DVyq0Uf1F	49	7
-- BE1kLRi26lUIQuHZlfimBaKy9wWj	49	32
-- IpV8_PQSMFFJAHvYxlsgy4m75Giy	48	23

select * from lv_stats where browser_id in ('SLrH5X9gn_dAdDB4LxUblGuf07wh') group by all 


-- TEST 2: make sure browsers match engagement. use the ctes + events table to be sure
----browsers + listings w/ engagement
----browsers + listings w/o engagement
