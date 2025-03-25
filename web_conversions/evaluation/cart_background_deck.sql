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
with event_ordered as (
select
  platform,
	visit_id,
  sequence_number,
	event_type,
	lead(event_type) over (partition by visit_id, listing_id order by sequence_number) as next_event,
	lead(sequence_number) over (partition by visit_id, listing_id order by sequence_number) as next_sequence_number
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 1=1
  and platform in ('boe','mobile_web','desktop')
  and v._date >= current_date-30
  and page_view =1 -- primary pages only 
)
select
  platform,
  event_type,
  count(distinct visit_id) as visits,
  count(sequence_number) as pageviews 
from 
  event_ordered
where 
  next_event in ('cart_view')
group by all

-- gms 
