--create table with all visit_id + browser_id info i might need
create or replace table etsy-data-warehouse-dev.madelinecollins.cart_engagement_browsers as (
select
  platform,
  browser_id,
  visit_id,
  converted,
  cart_adds,
  case when v.user_id is null or v.user_id = 0 then 0 else 1 end as buyer_segment,
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

-- how many visits to cart will a browser have over one month?
with agg as (
select
  platform,
  -- buyer_segment,
  -- new_visitor, 
  browser_id,
  count(visit_id) as cart_views,
from 
    etsy-data-warehouse-dev.madelinecollins.cart_engagement_browsers
group by all
)
select 
  platform,
  -- buyer_segment,
  -- new_visitor, 
  count(distinct browser_id) as browsers,
  avg(cart_views) as avg_cart_views,
  sum(cart_views) as cart_views
from 
  agg
group by all 


