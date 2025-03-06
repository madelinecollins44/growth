--------------
--OPP SIZING
--------------
-- total traffic counts
select
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_visits
from 
  etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30

-- visits with view_listing event
select
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_visits
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 
  v._date >= current_date-30
  and e.event_type in ('view_listing')
  
-- visits with review photo clicks (appreciation_photo_overlay_opened)
select
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_visits
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 
  v._date >= current_date-30
  and e.event_type in ('appreciation_photo_overlay_opened')

-- total traffic counts + converted
select
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_visits
from 
  etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30
  and v.converted > 0

-- visits with view_listing event + converted
select
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_visits
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 
  v._date >= current_date-30
  and e.event_type in ('view_listing')
  and v.converted > 0

  
-- visits with review photo clicks (appreciation_photo_overlay_opened) + converted
select
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_visits
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 
  v._date >= current_date-30
  and e.event_type in ('appreciation_photo_overlay_opened')
  and v.converted > 0
