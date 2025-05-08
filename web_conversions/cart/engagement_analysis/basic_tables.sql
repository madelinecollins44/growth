--create table with all visit_id + browser_id info i might need
create or replace table etsy-data-warehouse-dev.madelinecollins.cart_engagement_browsers as (
select
  platform,
  browser_id,
  visit_id,
  case when v.user_id is null or v.user_id = 0 then 'signed_out' else 'signed_in' end as buyer_segment,
  new_visitor
from
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where
  v._date >= current_date-30
  and platform in ('desktop','mobile_web')
  and event_type in ('cart_view')
); 
