/* START WITH UNDERSTANDING BASIC STATS AROUND THE CART PAGE*/
-- total cart traffic
select 
  platform, 
  count(distinct visit_id) as visits,
  count(distinct case when event_type in ('cart_view') then visit_id end) as cart_visits
from etsy-data-warehouse-prod.weblog.visits v
inner join etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 1=1
  and platform in ('boe','mobile_web','desktop')
  and v._date >= current_date-30
group by all 

-- referring page
-- gms 
