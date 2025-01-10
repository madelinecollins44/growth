--------------
--OPP SIZING
--------------
-- total traffic counts
select
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_visits,  
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_visits
from 
  etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30

-- visits with lp review seen event 
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
  and e.event_type in ('listing_page_reviews_seen')
