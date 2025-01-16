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


--trying this w review_start date
with reviews as (
select
  transaction_id,
  date_diff(date(review_date),date(review_start),day) as transaction_to_review,
  -- date_diff(date(review_date),date(shipped_date),day) as shipped_to_review
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
where has_review =1
)
select 
  transaction_to_review,
  count(distinct transaction_id) as transactions
from 
  reviews
where transaction_to_review > 100
group by all 
order by 1 asc


--testing to make sure day calc is right

select transaction_id, date(review_date), date(review_start) from  etsy-data-warehouse-prod.rollups.transaction_reviews
where transaction_id in (1451147833,2030692255,2039775796,2078557855)
