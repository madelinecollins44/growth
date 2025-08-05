with first_atc as (
select
  visit_id,
  -- split(visit_id, ".")[0] as browser_id, 
  -- min(sequence_number) as _min
from 
  etsy-data-warehouse-prod.analytics.listing_views 
where 
  _date >= current_date-30
  and platform in ('boe','mobile_web','desktop')
  and added_to_cart = 1
)
