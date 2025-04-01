/* PURPOSE: this funnel is meant to help us understand if there is a correlation between review engagement and conversion, and if this track of work is worth continuing. */

--total visits in the last 30 days
select 
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_traffic,
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_traffic,
from 
  etsy-data-warehouse-prod.weblog.visits 
where 
 _date >= current_date-30
 group by all 
  
--listing views
select
  count(distinct visit_id) as visits_w_lv,
  count(visit_id) as listing_views,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_visits_w_lv,
  count(case when platform in ('desktop') then visit_id end) as desktop_listing_views,
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_visits_w_lv,
  count(case when platform in ('mobile_web') then visit_id end) as mweb_listing_views,
from 
  etsy-data-warehouse-prod.analytics.listing_views
where
  _date >= current_date-30
