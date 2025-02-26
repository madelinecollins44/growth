---------------------------------------------------------------
-- OVERALL TRAFFIC COUNTS 
---------------------------------------------------------------
-- total traffic counts
select
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_visits,  
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_visits
from 
  etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30


-- visits with shop home
select
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_visits,  
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_visits
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 
  v._date >= current_date-30
  and e.event_type in ('shop_home')

---------------------------------------------------------------
-- LOOKING INTO SHOP HOME SPECIFIC
---- viewport_width: tv, xl, lg sizes (900px +) 
---- exclude any visits associated with a seller account
---- exclude any visits to an etsy plus shop home page
---- desktop only
---------------------------------------------------------------
begin
create or replace temp table web_shop_home_visits as (
select 
  platform,
  beacon.event_name,
  viewport_width,
  (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id, 
  (select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id, 
  visit_id, 
  sequence_number,
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
inner join 
  etsy-data-warehouse-prod.weblog.visits using (visit_id)
where
  date(_partitiontime) >= current_date-30
  and _date >= current_date-30
  and platform in ('desktop')
  and (beacon.event_name in ('shop_home')) -- pull all shop_home 
group by all
);
end

---------------------------------------------------------------
-- TESTING
---------------------------------------------------------------
-- TEST 1: confirm visits + pageviews from temp table match weblog.events
select
  count(distinct visit_id) as desktop_visits,  
  count(visit_id) as desktop_pageviews,  
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 
  v._date >= current_date-30
  and e.event_type in ('shop_home')
  and platform in ('desktop')
--   desktop_visits	desktop_pageviews
-- 87680454	140244508

select 
  count(distinct visit_id) as visits,
  count(visit_id) as pageviews
from etsy-bigquery-adhoc-prod._scriptc69a8e301bcb5a780c249f40ff0e44a2cbf09faf.web_shop_home_visits
-- visits	pageviews
-- 87680454	140244508
