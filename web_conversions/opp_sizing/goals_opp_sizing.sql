-------------------------------------------------------
--GLOBAL COVERAGE
-------------------------------------------------------
select
  count(distinct visit_id) as total_visits,
  sum(total_gms) as gms
from etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30
group by all
-- total_visits	gms
-- 1130685375	987510850.32

select
  count(distinct visit_id) as total_visits,
  sum(total_gms) as gms
from etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-90
group by all
-- total_visits	gms
-- 3286564937	2757370099.95

-------------------------------------------------------
--LISTING PAGE LANDINGS (last 90 days)
-------------------------------------------------------
select
  count(distinct visit_id) as listing_landing_visits,
  sum(total_gms) as listing_landing_gms
from 
  etsy-data-warehouse-prod.weblog.visits 
where 
  _date >= current_date-90
  and platform in ('mobile_web','desktop')
  and landing_event in ("view_listing", "image_zoom",'listing_page_recommendations','view_sold_listing','view_unavailable_listing','listing__listing_hub__tapped','appreciation_photo_detail')
group by all
-- listing_landing_visits	listing_landing_gms
-- 1089366238	537405474.99
-- 33.14% of visit coverage, 19.48% of gms coverage


-------------------------------------------------------
--SHOP HOME VISITS (last 30 days)
-------------------------------------------------------
with shop_home_visits as (
select
  distinct visit_id
from 
  etsy-data-warehouse-prod.weblog.events
where 
  _date >= current_date-30
  and event_type in ('shop_home')
)
select
  count(distinct a.visit_id) as shop_home_visits,
  sum(total_gms) as shop_home_gms
from 
  shop_home_visits a
inner join 
  etsy-data-warehouse-prod.weblog.visits b using (visit_id)
where 
  b._date >= current_date-30
  and b.platform in ('mobile_web','desktop')
group by all
-- shop_home_visits	shop_home_gms
-- 118112155	188864678.03
---10.45% of visit coverage, 19.13% of gms coverage

-------------------------------------------------------
--LISTING PAGE (last 30 days)
-------------------------------------------------------
with lp_visits as (
select
  distinct visit_id
from 
  etsy-data-warehouse-prod.weblog.events
where 
  _date >= current_date-30
  and event_type in ('view_listing')
)
select
  count(distinct a.visit_id) as lp_visits,
  sum(total_gms) as lp_gms
from 
  lp_visits a
inner join 
  etsy-data-warehouse-prod.weblog.visits b using (visit_id)
where 
  b._date >= current_date-30
  and b.platform in ('mobile_web','desktop')
group by all
-- lp_visits	lp_gms
-- 474065465	516725816.26
--- 47.2% of visit coverage, 51.4% of gms coverage
-------global visits / gms coverage for this calc 
-- total_visits	gms
-- 1140444332	1004663981.54

-------------------------------------------------------
--LISTING PAGE WHERE LISTING HAD A REVIEW (last 30 days)
-------------------------------------------------------
with reviews as (
select
  listing_id,
  sum(has_review) as total_reviews
from etsy-data-warehouse-prod.rollups.transaction_reviews
group by all
)
, lv_with_reviews as (
select
  visit_id,
  count(listing_id) as listings_w_review
from  
  etsy-data-warehouse-prod.analytics.listing_views lv
inner join 
  reviews r using (listing_id)
where 
  lv._date >= current_date-30 -- listing views in last 30 days 
  and r.total_reviews > 0 -- only looks at listings with reviews  
group by all 
)
select
  count(distinct visit_id) as visits,
  sum(total_gms) as gms
from 
  etsy-data-warehouse-prod.weblog.visits
inner join 
  lv_with_reviews 
   using (visit_id)
where 
    _date >= current_date-30
    and platform in ('mobile_web','desktop')
-- visits	gms
-- 351153936	456158232.69
-- 30.1 of visit coverage, 45.4% of gms coverage 
-------global visits / gms coverage for this calc 
-- total_visits	gms
-- 1140444332	1004663981.54

-------------------------------------------------------
--LISTING REVIEWS SEEN (last 30 days)
-------------------------------------------------------
with lp_reviews_seen as (
select
  distinct visit_id
from 
  etsy-data-warehouse-prod.weblog.events
where 
  _date >= current_date-30
  and event_type in ('listing_page_reviews_seen')
)
select
  count(distinct a.visit_id) as lp_reviews_seen_visits,
  sum(total_gms) as lp_reviews_seen_gms
from 
  lp_reviews_seen a
inner join 
  etsy-data-warehouse-prod.weblog.visits b using (visit_id)
where 
  b._date >= current_date-30
  and b.platform in ('mobile_web','desktop')
group by all
-- lp_reviews_seen_visits	lp_reviews_seen_gms
-- 140509906	330730550.96
-- 12.3% of visit coverage, 32.9% of gms coverage 
-------global visits / gms coverage for this calc 
-- total_visits	gms
-- 1140444332	1004663981.54
