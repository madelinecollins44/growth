select 
  platform,
  count(distinct case when event_type in ('shop_home') then visit_id end) as shop_home_visits,
  count(distinct case when event_type in ('shop_home_about_section_seen') then visit_id end) as section_visits,
  count(case when event_type in ('shop_home') then sequence_number end) as shop_home_views,
  count(case when event_type in ('shop_home_about_section_seen') then sequence_number end) as section_views,
from 
  etsy-data-warehouse-prod.weblog.events e
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where   
  v._date >= current_date-30
  and event_type in ('shop_home_about_section_seen','shop_home')
group by all 
