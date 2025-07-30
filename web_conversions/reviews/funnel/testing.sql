-- MAKE SURE % OF REVIEWS WITH A PHOTO MATCH 
select  
  count(distinct transaction_id) as transactions,
  count(distinct case when has_review > 0 then transaction_id end) as trans_w_review,
  count(distinct case when has_image > 0 then transaction_id end) as trans_w_image,
  count(distinct case when has_text_review > 0 then transaction_id end) as has_text_review,

from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
