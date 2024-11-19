----------------------------------------------------------------------------------
-- How many sellers have item reviews? 
-----1-5, 5-10, 11+?
----------------------------------------------------------------------------------
with seller_listing_reviews as (
select
  seller_user_id,
  sum(has_review) as total_listing_reviews,
  count(listing_id) as listings_purchased,
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


-----------------TESTING
with seller_listing_reviews as (
select
  seller_user_id,
  sum(has_review) as total_listing_reviews
from etsy-data-warehouse-prod.rollups.transaction_reviews
group by all 
) 
select * from seller_listing_reviews where total_listing_reviews =11 limit 5
-- seller_user_id	total_listing_reviews
-- 599451425	470
-- 248686804	639
-- 842003194	0
-- 159140018	0
-- 39374340	11
-- 76891391	11

SELECT * FROM etsy-data-warehouse-prod.rollups.transaction_reviews WHERE SELLER_USER_ID = 76891391

----------------------------------------------------------------------------------
-- How many sellers have shop reviews? 
-----1-5, 5-10, 11+?
----------------------------------------------------------------------------------

