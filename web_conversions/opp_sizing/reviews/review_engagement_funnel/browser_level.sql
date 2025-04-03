-- find which browsers viewed 
with lv_stats as (
select 
  platform,
  split(visit_id,'-')[safe_offset(0)] as browser_id, 
  listing_id,
  visit_id,
  count(visit_id) as listing_views,
  count(distinct visit_id) as unique_visits,
  sum(purchased_after_view) as purchases
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  platform in ('desktop','mobile_web')
  and _date >= current_date-30
group by all 
)
, review_engagements as (
select
  s.platform,
  beacon.browser_id,
  b.visit_id,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id,
  count(sequence_number) as engagements,
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` b
inner join -- join here to get platform and only look at browsers that have viewed a listing 
  lv_stats s
    on s.browser_id= b.beacon.browser_id
    and s.visit_id= b.visit_id
where
	date(_partitiontime) >= current_date-30
	and (beacon.event_name in ("listing_page_reviews_pagination","appreciation_photo_overlay_opened") --all these events are lp specific 
      or (beacon.event_name) in ("sort_reviews") and (select value from unnest(beacon.properties.key_value) where key = "primary_event_source") in ('view_listing'))  -- sorting on listing page 
group by all 
)
select
  s.platform,
--lv stats
  count(distinct s.browser_id) as browsers_w_lv,
  count(distinct case when purchases > 0 then s.browser_id end) as browsers_w_purchase,
  count(distinct s.visit_id) as visits_w_lv,
  count(distinct case when purchases > 0 then s.visit_id end) as visits_w_purchase,
  sum(listing_views) as total_lv,
  sum(purchases) as total_purchases,
-- engagement stats
  count(distinct r.browser_id) as browsers_w_engagement,
  count(distinct case when purchases > 0 then r.browser_id end) as engaged_browsers_w_purchase,
  sum(engagements) as total_engagements,
  count(distinct r.visit_id) as visits_w_engagement,
   count(distinct case when purchases > 0 then r.visit_id end) as engaged_visits_w_purchase,
from 
  lv_stats s
left join 
  review_engagements r
    on s.browser_id=r.browser_id
    and cast(s.listing_id as string)=r.listing_id
    and s.platform=r.platform
    and s.visit_id=r.visit_id 
group by all 



-----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- TESTING 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
/* -- gather listing stats  
create or replace table etsy-data-warehouse-dev.madelinecollins.browser_level_engagements as (
with lv_stats as (
select 
  platform,
  split(visit_id,'-')[safe_offset(0)] as browser_id, 
  listing_id,
  visit_id,
  count(visit_id) as listing_views,
  count(distinct visit_id) as unique_visits,
  sum(purchased_after_view) as purchases
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  platform in ('desktop','mobile_web')
  and _date >= current_date-4
group by all 
)
-- look at review engagements on specific listings 
, review_engagements as (
select
  s.platform,
  beacon.browser_id,
  b.visit_id, -- need this to keep platform straight at a browser can view the same listing across platforms 
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id,
  count(sequence_number) as engagements,
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` b
inner join -- join here to get platform and only look at browsers that have viewed a listing 
  lv_stats s
    on s.browser_id= b.beacon.browser_id
    and s.visit_id= b.visit_id
where
	date(_partitiontime) >= current_date-4
	and (beacon.event_name in ("listing_page_reviews_pagination","appreciation_photo_overlay_opened") --all these events are lp specific 
      or (beacon.event_name) in ("sort_reviews") and (select value from unnest(beacon.properties.key_value) where key = "primary_event_source") in ('view_listing'))  -- sorting on listing page 
group by all 
)
select
  s.platform,
  s.browser_id, 
  s.listing_id,
--lv stats
  count(distinct s.browser_id) as browsers_w_lv,
  count(distinct case when purchases > 0 then s.browser_id end) as browsers_w_purchase,
  count(distinct s.visit_id) as visits_w_lv,
  count(distinct case when purchases > 0 then s.visit_id end) as visits_w_purchase,
  sum(listing_views) as total_lv,
  sum(purchases) as total_purchases,
-- engagement stats
  count(distinct r.browser_id) as browsers_w_engagement,
  count(distinct case when purchases > 0 then r.browser_id end) as engaged_browsers_w_purchase,
  sum(engagements) as total_engagements,
  count(distinct r.visit_id) as visits_w_engagement,
   count(distinct case when purchases > 0 then r.visit_id end) as engaged_visits_w_purchase,
from 
  lv_stats s
left join 
  review_engagements r
    on s.browser_id=r.browser_id
    and cast(s.listing_id as string)=r.listing_id
    and s.platform=r.platform
    and s.visit_id=r.visit_id 
group by all 
);
 */

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
