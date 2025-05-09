-- how many visits to cart will a browser have over one month?
with agg as (
select
  platform,
  -- buyer_segment,
  -- new_visitor, 
  browser_id,
  count(visit_id) as visits_w_cart,
  sum(cart_views) as total_views
from 
    etsy-data-warehouse-dev.madelinecollins.cart_engagement_browsers
group by all
)
select 
  platform,
  -- buyer_segment,
  -- new_visitor, 
  count(distinct browser_id) as browsers,
  sum(visits_w_cart) as visits_w_cart,
  avg(total_views) as avg_cart_views,
  sum(total_views) as cart_views
from 
  agg
group by all 
