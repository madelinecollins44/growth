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

select
  count(distinct visit_id) as total_visits,
  count(distinct case when converted > 0 then visit_id end) as converted,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_visits,  
  count(distinct case when platform in ('desktop') and converted > 0 then visit_id end) as desktop_converted
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

select
  count(distinct visit_id) as total_visits,
  count(distinct case when converted > 0 then visit_id end) as converted,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_visits,  
  count(distinct case when platform in ('desktop') and converted > 0 then visit_id end) as desktop_converted
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 
  v._date >= current_date-30
  and e.event_type in ('shop_home')
