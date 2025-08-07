/* create or replace table etsy-data-warehouse-dev.madelinecollins.holder_table as (
select  
  platform,
  split(visit_id, ".")[0] as browser_id, 
  visit_id,
  sequence_number, 
  listing_id, 
  added_to_cart,
  row_number() over (order by visit_id) AS visit_order
from 
  etsy-data-warehouse-prod.analytics.listing_views 
where 
  _date >= current_date-14
  and platform in ('boe','mobile_web','desktop')
); */
