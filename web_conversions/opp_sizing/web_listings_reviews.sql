--total desktop visits in the last 30 days
select 
  -- case when user_id is not null then 1 else 0 end as is_signed_in,
  count(distinct visit_id) 
from 
  etsy-data-warehouse-prod.weblog.visits 
where 
 _date >= current_date-30
 and platform in ('desktop')
 group by all 
