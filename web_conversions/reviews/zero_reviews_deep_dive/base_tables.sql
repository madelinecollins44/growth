with shops_wo_reviews as (
select 
  shop_id,
  shop_name,
  seller_user_id,
  count(distinct transaction_id) as transactions,
  sum(has_review) as total_reviews
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
inner join  
  etsy-data-warehouse-prod.rollups.seller_basics using (shop_id)
group by all 
having sum(has_review) = 0
)
select * from shops_wo_reviews order by transactions desc limit 40
