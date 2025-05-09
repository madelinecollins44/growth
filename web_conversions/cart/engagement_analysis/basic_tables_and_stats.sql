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


