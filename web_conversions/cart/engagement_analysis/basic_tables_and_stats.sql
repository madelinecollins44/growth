--create table with all visit_id + browser_id info i might need
create or replace table etsy-data-warehouse-dev.madelinecollins.cart_engagement_browsers as (
select
  platform,
  browser_id,
  visit_id,
  converted,
  cart_adds,
  case when v.user_id is null or v.user_id = 0 then 0 else 1 end as buyer_segment,
  new_visitor,
  count(sequence_number) as cart_views
from
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where
  v._date >= current_date-30
  and platform in ('desktop','mobile_web','boe')
  and event_type in ('cart_view')
group by all 
); 


with key_events as (
select
  distinct visit_id
from 
  etsy-data-warehouse-prod.weblog.events
where 
  _date >= current_date-30 
  and event_type in ('cart_view')
)
select
  platform,
  new_visitor,
  case when v.user_id is null or v.user_id = 0 then 0 else 1 end as buyer_segment,
  count(distinct browser_id) as browsers,
  count(distinct visit_id) as visits,
  sum(total_gms) as total_gms, 
  count(distinct case when ev.visit_id is not null then browser_id end) as cart_browsers,
  count(distinct case when ev.visit_id is not null then visit_id end) as cart_visits,
  sum(case when ev.visit_id is not null then total_gms end) as cart_gms,
from 
  etsy-data-warehouse-prod.weblog.visits v
left join 
  key_events ev using (visit_id)
where 
  v._date >= current_date-30
group by all 
  
