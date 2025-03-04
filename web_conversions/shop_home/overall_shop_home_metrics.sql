---traffic, conversions
  select
  platform,
  count(distinct v.visit_id) as total_traffic,
  count(distinct case when event_type in ('shop_home') then visit_id end) as shop_home_traffic,
  count(distinct case when converted > 0 then visit_id end) as converted_traffic,
  count(distinct case when converted > 0 and event_type in ('shop_home')then visit_id end) as shop_home_converted_traffic,
from etsy-data-warehouse-prod.weblog.visits v
left join etsy-data-warehouse-prod.weblog.events e using (visit_id)
where v._date >= current_date-30
and platform in ('desktop','mobile_web','boe')
group by all 

--gms from shop visits 
-- shop home gms
with key_events as (
select
  distinct visit_id
from 
  etsy-data-warehouse-prod.weblog.events
where 
  _date >= current_date-30 
  -- and event_type in ('view_listing')
  and event_type in ('shop_home')
  -- and event_type in ('cart_view')
  -- and event_type in ('home','homescreen')
  -- and event_type in ('recommended')
  -- and event_type in ('search')
  -- and event_type in ('market')
)
select  
  platform,
  sum(total_gms) as total_gms, 
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join key_events ev using (visit_id)
where v._date >= current_date-30
group by all 

-- total gms across platforms
select  
  platform,
  sum(total_gms) as total_gms, 
from 
  etsy-data-warehouse-prod.weblog.visits v
where v._date >= current_date-30
group by all 
