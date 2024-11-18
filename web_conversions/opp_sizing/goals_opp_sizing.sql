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
--LISTING LANDINGS
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
--SHOP HOME VISITS 
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
