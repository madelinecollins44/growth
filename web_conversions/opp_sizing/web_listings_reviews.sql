--total desktop visits in the last 30 days
select 
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_traffic,
  count(distinct case when user_id is not null and platform in ('desktop') then visit_id end) as signedin_desktop_traffic,
  count(distinct case when user_id is null and platform in ('desktop') then visit_id end) as signedout_desktop_traffic,
from 
  etsy-data-warehouse-prod.weblog.visits 
where 
 _date >= current_date-30
 and platform in ('desktop')
 group by all 

  
--desktop listing views
select
  count(distinct visit_id) as visits_w_lv,
  count(visit_id) as listing_views
from 
  etsy-data-warehouse-prod.analytics.listing_views
where
  _date >= current_date-30
  and platform in ('desktop')
