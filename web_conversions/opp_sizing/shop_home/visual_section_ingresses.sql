---------------------------------------------------------------
-- OVERALL TRAFFIC COUNTS 
---------------------------------------------------------------
select
  count(distinct visit_id) as total_visits,
  count(distinct case when converted > 0 then visit_id end) as converted,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_visits,  
  count(distinct case when platform in ('desktop') and converted > 0 then visit_id end) as desktop_converted,
  count(distinct case when platform in ('mobile_web') then visit_id end) as mobile_web_visits,  
  count(distinct case when platform in ('mobile_web') and converted > 0 then visit_id end) as mobile_web_converted
from 
  etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30

-- visits with shop home
select
  count(distinct visit_id) as total_visits,
  count(distinct case when converted > 0 then visit_id end) as converted,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_visits,  
  count(distinct case when platform in ('desktop') and converted > 0 then visit_id end) as desktop_converted,
  count(distinct case when platform in ('mobile_web') then visit_id end) as mobile_web_visits,  
  count(distinct case when platform in ('mobile_web') and converted > 0 then visit_id end) as mobile_web_converted
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 
  v._date >= current_date-30
  and e.event_type in ('shop_home')
