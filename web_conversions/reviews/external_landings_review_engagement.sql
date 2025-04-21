--
with all_lv as (
select 
  case when url like ('%external=1%') then 1 else 0 end as external_listing,
  visit_id, 
  sequence_number, 
  purchased_after_view,
from 
  etsy-data-warehouse-prod.weblog.events e
inner join 
  etsy-data-warehouse-prod.analytics.listing_views lv using (visit_id, sequence_number, listing_id)
where 
  event_type in ('view_listing') 
  and lv_date >= current_date-30
