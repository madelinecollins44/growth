with all_receipts as (
select
  buyer_user_id,
  receipt_id,
  initial_edd_min,
  date_diff(initial_edd_min, current_date,day) as dates_between_edd
from etsy-data-warehouse-prod.rollups.receipt_shipping_basics
group by 1,2,3
)
select * 
from all_receipts
where dates_between_edd <= 100 -- can leave reviews up to 100 days after edd
