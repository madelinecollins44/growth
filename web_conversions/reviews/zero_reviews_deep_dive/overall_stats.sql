------------------------------------------------------------------------------
-- HOW MANY ACTIVE SHOPS DONT HAVE REVIEWS? 
------------------------------------------------------------------------------

with shop_reviews as ( -- this looks at all listings that have been purchased and whether or not they have a review
select 
  shop_id,
  seller_user_id,
  count(distinct transaction_id) as transactions,
  sum(has_review) as total_reviews
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
group by all 
)
select
  case when total_reviews = 0 or r.seller_user_id is null then 0 else 1 end as has_shop_reviews,
  count(distinct b.shop_id) as active_shops
from  
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  shop_reviews r
   using (shop_id)
where 
  active_seller_status = 1  -- only active sellers 
group by all 
