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
-- create table for all shop home visits and banner info, etsy plus status, and seller visit info
create or replace table etsy-data-warehouse-dev.madelinecollins.web_shop_home_traffic_opp_sizing as (
with visits as (
select 
  v.platform,
  is_seller,
  converted,
  beacon.event_name,
  case when viewport_width >= 900 then 1 else 0 end as lg_plus_screen_size,
  (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id, 
  (select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id, 
  b.visit_id, 
  b.sequence_number,
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` b
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile p 
    on v.user_id=p.user_id
where
  date(_partitiontime) >= current_date-30
  and _date >= current_date-30
  and platform in ('desktop')
  and (beacon.event_name in ('shop_home')) -- pull all shop_home 
group by all
)
, etsy_plus_status as (
select
  is_etsy_plus,
  user_id as seller_user_id,
  shop_id 
from 
  etsy-data-warehouse-prod.rollups.seller_basics
where 
  active_seller_status = 1 
)
, shop_banners as (
select
  shop_id,
  user_id as seller_user_id,
  case when branding_option > 0 then 1 else 0 end as has_banner
from 
  etsy-data-warehouse-prod.etsy_shard.shop_data
group by all 
)
select
  v.*,
  is_etsy_plus
from 
  visits v
left join 
  etsy_plus_status ep 
    on cast(ep.shop_id as string)=v.shop_id
    and ep.seller_user_id=ep.seller_user_id
left join 
  shop_banners b
    on cast(b.shop_id as string)=v.shop_id
    and b.seller_user_id=ep.seller_user_id
);



---------------------------------------------------------------
-- TESTING
---------------------------------------------------------------
-- TEST 1: confirm visits + pageviews from table match weblog.events
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
from etsy-data-warehouse-dev.madelinecollins.web_shop_home_traffic_opp_sizing
-- visits	pageviews
-- 87680454	140244508

-- TEST 2: test visits on visit_id level
select visit_id, count(*) from etsy-data-warehouse-dev.madelinecollins.web_shop_home_traffic_opp_sizing group by all order by 2 desc limit 5
-- visit_id	f0_
-- WbtCZohzj0J0UQyL7edwRCocHcLW.1740029073763.2	7210
-- HAZGJf8xWWeiEUqd_JXqx9fpDHGI.1740111012482.1	6972
-- HAZGJf8xWWeiEUqd_JXqx9fpDHGI.1740124600192.2	6801
-- kXKQm5w_5eLViJlHveqV-A-3y49i.1740143336890.1	6667
-- zQIP2G8sThj3oC9s9NBBGqU-rAky.1740489457820.1	6667

select visit_id, count(visit_id) from etsy-data-warehouse-prod.weblog.events where event_type in ('shop_home') and visit_id in ('WbtCZohzj0J0UQyL7edwRCocHcLW.1740029073763.2','HAZGJf8xWWeiEUqd_JXqx9fpDHGI.1740111012482.1') group by all
-- visit_id	f0_
-- HAZGJf8xWWeiEUqd_JXqx9fpDHGI.1740111012482.1	6972
-- WbtCZohzj0J0UQyL7edwRCocHcLW.1740029073763.2	7210
