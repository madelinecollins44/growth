----------------------------------------------------------------------------------
-- Comparing tables
----------------------------------------------------------------------------------
select count(distinct seller_user_id), count(distinct transaction_id), max(date) from etsy-data-warehouse-prod.transaction_mart.all_transactions
-- 8722260,	3408219171, 2024-11-20

select count(distinct seller_user_id), count(distinct transaction_id), max(transaction_date) from etsy-data-warehouse-prod.rollups.transaction_reviews
-- 7643311,	3062444660, 2024-11-20

---------------------------------------------------------------------------------
-- What % of sellers dont have a transaction? 
----------------------------------------------------------------------------------
select
  count(distinct b.user_id) as total_sellers,
  count(distinct r.seller_user_id) as sellers_w_trans,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b 
left join 
  etsy-data-warehouse-prod.transaction_mart.all_transactions r 
    on b.user_id=r.seller_user_id
where active_seller_status = 1
group by all
-- total_sellers	sellers_w_trans
-- 6011692	3302676
-- select 1-(3302676/6011692) --> 45.5%
  
----------------------------------------------------------------------------------
-- What % of reviews come from each buyer_segment? 
----------------------------------------------------------------------------------
select
  buyer_segment,
  sum(has_review) as total_listing_reviews,
  count(listing_id) as listings_purchased,
from etsy-data-warehouse-prod.rollups.transaction_reviews
group by all 
-- buyer_segment	    total_listing_reviews	        listings_purchased
-- Signed Out           76456111	                    655346082
-- Repeat	              171680871	                    795843899
-- Not Active	          93946331	                    595369827
-- High Potential	        1080826	                      7170958
-- Habitual	          187408259	                          741632160
-- Active	          47424564	                      265309720

----------------------------------------------------------------------------------
-- How many sellers have item reviews? 
-----1-5, 5-10, 11+?
----------------------------------------------------------------------------------
with seller_listing_reviews as (
select
  seller_user_id,
  sum(has_review) as total_listing_reviews,
  count(transaction_id) as transactions, 
  sum(quantity) as total_listing_purchased
from etsy-data-warehouse-prod.rollups.transaction_reviews
group by all 
) 
select
  -- seller_tier_new,
  count(distinct b.user_id) as total_sellers,
  count(distinct case when r.seller_user_id is null then b.user_id end) sellers_wo_transactions,
  count(distinct case when total_listing_reviews = 0 then b.user_id end) sellers_wo_listing_reviews,
  count(distinct case when total_listing_reviews >= 1 and total_listing_reviews < 5 then b.user_id end) sellers_w_1_to_5_listing_reviews,
  count(distinct case when total_listing_reviews >= 5 and total_listing_reviews < 10 then b.user_id end) sellers_w_5_to_10_listing_reviews,
  count(distinct case when total_listing_reviews >= 10 then b.user_id end) as sellers_w_10_or_more_listing_reviews
from 
  etsy-data-warehouse-prod.rollups.seller_basics b 
left join 
  seller_listing_reviews r 
    on b.user_id=r.seller_user_id
where active_seller_status = 1 -- only active sellers
group by all

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

with seller_listing_reviews as (
select
  seller_user_id,
  sum(has_review) as total_listing_reviews,
  count(transaction_id) as transactions, 
  sum(quantity) as total_listing_purchased
from etsy-data-warehouse-prod.rollups.transaction_reviews
group by all 
) 
select * from seller_listing_reviews where seller_user_id = 76891391
-- seller_user_id	total_listing_reviews	transactions	total_listing_purchased
-- 76891391	11	55	84
select * from etsy-data-warehouse-prod.rollups.transaction_reviews where seller_user_id = 76891391

----------------------------------------------------------------------------------
-- How many sellers have shop reviews? 
-----1-5, 5-10, 11+?
----------------------------------------------------------------------------------

