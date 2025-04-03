-----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- PULL DATA 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
----- total traffic last 30 days 
select 
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_visits,
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_visits,

  count(distinct browser_id) as total_browsers,
  count(distinct case when platform in ('desktop') then browser_id end) as desktop_browsers,
  count(distinct case when platform in ('mobile_web') then browser_id end) as mweb_browsers,
from 
  etsy-data-warehouse-prod.weblog.visits 
where 
 _date >= current_date-30
group by all 

----- total listing views
select
  count(distinct split(visit_id,'.')[safe_offset(0)]) as browsers_w_lv,
  count(visit_id) as listing_views,
  sum(purchased_after_view) as total_purchases,
  count(distinct case when platform in ('desktop') then split(visit_id,'.')[safe_offset(0)] end) as desktop_browsers_w_lv,
  count(distinct case when platform in ('desktop') and purchased_after_view > 0 then split(visit_id,'.')[safe_offset(0)] end) as desktop_browsers_w_purchase,
  count(case when platform in ('desktop') then visit_id end) as desktop_listing_views,
  sum(case when platform in ('desktop') then purchased_after_view end) as desktop_purchases,
  count(distinct case when platform in ('mobile_web') then split(visit_id,'.')[safe_offset(0)] end) as mweb_browsers_w_lv,
  count(distinct case when platform in ('mobile_web') and purchased_after_view > 0 then split(visit_id,'.')[safe_offset(0)] end) as mweb_browsers_w_purchase,
  count(case when platform in ('mobile_web') then visit_id end) as mweb_listing_views,
  sum(case when platform in ('mobile_web') then purchased_after_view end) as desktop_purchases,
from 
  etsy-data-warehouse-prod.analytics.listing_views
where
  _date >= current_date-30
group by all 
	
----- listing views w/ reviews
with listing_views as (
select
  platform,
  listing_id,
  split(visit_id,'.')[safe_offset(0)] as browser_id,
  count(visit_id) as listing_views
from 
  etsy-data-warehouse-prod.analytics.listing_views
where
  _date >= current_date-30
  and platform in ('desktop','mobile_web','boe')
group by all
)
, reviews as (
select
  listing_id,
  sum(has_review) as has_review,
  sum(has_image) as has_image,
  sum(has_video) as has_video,
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews  
group by all
)
select
  platform,
  sum(lv.listing_views) as total_listing_views,
  count(distinct lv.browser_id) as browsers_w_listing_view,
  count(distinct case when r.has_review > 0 then lv.listing_id end) listings_w_review,
  count(distinct case when r.has_image > 0 then lv.listing_id end) listings_w_image,
  count(distinct case when r.has_video > 0 then lv.listing_id end) listings_w_video,
  sum(case when r.has_review > 0 then listing_views end) has_review_lv,
  sum(case when r.has_image > 0 then listing_views end) has_image_lv,
  sum(case when r.has_video > 0 then listing_views end) has_video_lv,
  count(distinct case when r.has_review > 0 then lv.browser_id end) browsers_w_review,
  count(distinct case when r.has_image > 0 then lv.browser_id end) browsers_w_image,
  count(distinct case when r.has_video > 0 then lv.browser_id end) browsers_w_video,
from
  listing_views lv
left join 
  reviews r using (listing_id)
group by all
	


-----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- BROWSER ENGAGEMENTS 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
/* 
create or replace table etsy-data-warehouse-dev.madelinecollins.review_engagements as (
select
  beacon.browser_id,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id,
  count(sequence_number) as engagements,
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` b 
where
	date(_partitiontime) >= current_date-30
	and ((beacon.event_name in ("listing_page_reviews_pagination","appreciation_photo_overlay_opened") --all these events are lp specific 
      or (beacon.event_name) in ("sort_reviews") and (select value from unnest(beacon.properties.key_value) where key = "primary_event_source") in ('view_listing')))  -- sorting on listing page 
group by all 
);

create or replace table etsy-data-warehouse-dev.madelinecollins.lv_stats as (
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
  and _date >= current_date-30
group by all 
);

create or replace table etsy-data-warehouse-dev.madelinecollins.review_engagements_event_level as (
select
  beacon.browser_id,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id,
  beacon.event_name,
  count(sequence_number) as engagements,
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` b 
where
	date(_partitiontime) >= current_date-30
	and ((beacon.event_name in ("listing_page_reviews_pagination","appreciation_photo_overlay_opened") --all these events are lp specific 
      or (beacon.event_name) in ("sort_reviews") and (select value from unnest(beacon.properties.key_value) where key = "primary_event_source") in ('view_listing')))  -- sorting on listing page 
group by all 
);

*/
	
select
  s.platform,
--lv stats
  count(distinct s.browser_id) as browsers_w_lv,
  count(distinct case when purchases > 0 then s.browser_id end) as browsers_w_purchase,
  sum(listing_views) as total_lv,
  sum(purchases) as total_purchases,
-- engagement stats
  count(distinct r.browser_id) as browsers_w_engagement,
  count(distinct case when purchases > 0 then r.browser_id end) as engaged_browsers_w_purchase,
  sum(engagements) as total_engagements,
from 
  etsy-bigquery-adhoc-prod._scriptff50f4cf1cc12b75b545c2c66abca1b5c8d2e056.lv_stats s
left join 
  etsy-bigquery-adhoc-prod._script90f1d9a40ab51aa266471f3adf16181872881c72.review_engagements r
    on s.browser_id=r.browser_id
    and cast(s.listing_id as string)=r.listing_id
group by all 

-----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- TESTING 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
/* TEMP TABLES:
etsy-bigquery-adhoc-prod._scriptff50f4cf1cc12b75b545c2c66abca1b5c8d2e056.lv_stats : 30 DAY LV STATS
etsy-bigquery-adhoc-prod._script9b1b1d2b53f289cb4f1e48f56f21e0abc32b9288.lv_stats_3: 3 DAY LV STATS
etsy-bigquery-adhoc-prod._script888423cb0faeebec85cc7ea239f776eeef96ce02.review_engagements: ENGAGEMENT 30 DAYS */

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

/* with browsers as (
select
  s.browser_id,
  case when r.browser_id is not null then 1 else 0 end as engaged,
  sum(listing_views) as lv,
  sum(engagements) as engagements
from 
  etsy-bigquery-adhoc-prod._scriptff50f4cf1cc12b75b545c2c66abca1b5c8d2e056.lv_stats s
left join 
  etsy-bigquery-adhoc-prod._script90f1d9a40ab51aa266471f3adf16181872881c72.review_engagements r
    on s.browser_id=r.browser_id
    and cast(s.listing_id as string)=r.listing_id
group by all 
)
select
browser_id, lv, engagements
from browsers
where engagements > 0
group by all 
order by 2 desc limit 5 */


/* 
-- browsers without engagements 
browser_id	lv	engagements
2L5ALC3gNr37Mb-GHCGSNgEE5zZf	358029	
cBsPGR-6_X-rGg3Oa-SECJo7KXzq	303476	
vpbkGWQhHQOdp38f-Ys2DZvc_O83	221178	
xUnw_pLXew3DX9GQSVySn8NNbNWs	184437	
tSgS5Z655TX14R_dSpHiNlrTtpFb	105911	*/

/* 
-- browsers with engagements 
browser_id	lv	engagements
XOD7wp6qfKlBdHc9DZjcCB0deEIM	2037	5
iNeV7m08UwxeEcW_iKHM0Wnrn0h5	805	792
Ie-89MDXxFFQFY6elXU7U_W5-BFP	621	6
YRKIlKjNH8fLnLMMVky7ohC6ghI6	460	6
bIY4Hb0xWhetSAjmvuilNiUEMUYW	448	3 */

select * from etsy-data-warehouse-prod.weblog.events 
where split(visit_id,'.')[safe_offset(0)] in ('bIY4Hb0xWhetSAjmvuilNiUEMUYW')
and _date >= current_date-30 
and event_type in ("sort_reviews", "listing_page_reviews_pagination","appreciation_photo_overlay_opened")

