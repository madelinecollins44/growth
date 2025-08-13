select
  receipt_id,
  initial_edd_min,
  date_diff(initial_edd_min, current_date,day) as elgible_for_review
from etsy-data-warehouse-prod.rollups.receipt_shipping_basics
group by 1,2
