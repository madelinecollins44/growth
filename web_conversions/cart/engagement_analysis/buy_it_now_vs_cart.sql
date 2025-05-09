select
  platform,
  case when v.user_id is null or v.user_id = 0 then 0 else 1 end as buyer_segment,
  new_visitor, 
  -- browser metrics 
  count(distinct case when event_type in ('cart_view') then browser_id end) as browsers_w_cart_view,
  count(distinct case when event_type in ('main_buybox_express_checkout_clicked') then browser_id end) as browsers_w_buy_it_now,
  count(distinct case when event_type in ('main_buybox_express_checkout_clicked','cart_view') then browser_id end) as browsers_w_both,
  -- visit metrics 
  count(distinct case when event_type in ('cart_view') then visit_id end) as visits_w_cart_view,
  count(distinct case when event_type in ('main_buybox_express_checkout_clicked') then visit_id end) as visits_w_buy_it_now,
  count(distinct case when event_type in ('main_buybox_express_checkout_clicked','cart_view') then visit_id end) as visits_w_both,
  count(case when event_type in ('cart_view') then visit_id end) as cart_views,
  count(case when event_type in ('main_buybox_express_checkout_clicked') then visit_id end) as buy_it_nows,
from
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where
  v._date >= current_date-30
  and platform in ('desktop','mobile_web','boe')
  and event_type in ('cart_view','main_buybox_express_checkout_clicked')
  and converted > 0 -- only converted visits 
group by all 
