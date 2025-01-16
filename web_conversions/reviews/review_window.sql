with reviews as (
select
  transaction_id,
  date_diff(date(review_date),date(transaction_date),day) as transaction_to_review,
  date_diff(date(review_date),date(shipped_date),day) as shipped_to_review
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
where has_review =1
)
select 
  transaction_to_review,
  count(distinct transaction_id)
from 
  reviews
group by all 
order by 1 desc
