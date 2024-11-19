----------------------------------------------------------------------------------
-- How many sellers have item reviews? 
-----1-5, 5-10, 11+?
----------------------------------------------------------------------------------
with seller_listing_reviews as (
select
  seller_user_id,
  sum(has_review) as total_listing_reviews
from etsy-data-warehouse-prod.rollups.transaction_reviews
group by all 
) 
select
  count(distinct b.user_id) as total_sellers,
  count(distinct case when total_listing_reviews >= 1 or total_listing_reviews < 5 then b.user_id end) sellers_w_1_to_5_listing_reviews,
  count(distinct case when total_listing_reviews >= 5 or total_listing_reviews < 10 then b.user_id end) sellers_w_5_to_10_listing_reviews,
  count(distinct case when total_listing_reviews >= 10 then b.user_id end) as sellers_w_10_or_more_listing_reviews
from 
  etsy-data-warehouse-prod.rollups.seller_basics b 
left join 
  seller_listing_reviews r 
    on b.user_id=r.seller_user_id

----------------------------------------------------------------------------------
-- How many sellers have shop reviews? 
-----1-5, 5-10, 11+?
----------------------------------------------------------------------------------

