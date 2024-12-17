--------------------------------------------------
-- HOW MANY WORDS IS A REVIEW ON AVERAGE
--------------------------------------------------
--funnel / abandonment like for the 3 step review submission process 

--------------------------------------------------
-- SHOP HOME VS LISTING PAGE REVIEW COMPARISON
--------------------------------------------------
-- click stars to access review component 
-- pagination 
-- # of reviews, avg rating
-- review photos / videos

-----------------------------------------------------------------------------------------------------------------------
-- LISTING COMPARISON
---- What % of listings and listing views have photos, videos, seller responses and is there a CR difference/impact?
-----------------------------------------------------------------------------------------------------------------------

--------------------------------------------------
--QUALITIES OF A REVIEW
---- words on avg
---- have image
---- seller feedback 
--------------------------------------------------
-- word count, seller feedback
select
  count(distinct transaction_id) as transactions,
  count(distinct shop_id) as shops,
  count(distinct buyer_user_id) as buyers,
  avg(rating) as average_rating,
  count(case when rating = 0 then transaction_id end) as reviews_w_ratings_of_0,
  count(case when rating = 1 then transaction_id end) as reviews_w_ratings_of_1,
  count(case when rating = 2 then transaction_id end) as reviews_w_ratings_of_2,
  count(case when rating = 3 then transaction_id end) as reviews_w_ratings_of_3,
  count(case when rating = 4 then transaction_id end) as reviews_w_ratings_of_4,
  count(case when rating = 5 then transaction_id end) as reviews_w_ratings_of_5,
  count(case when seller_feedback != '' then transaction_id end) as reviews_w_seller_feedback,
  avg(array_length(split(seller_feedback, ' '))) AS avg_words_per_seller_feedback,
  avg(array_length(split(review, ' '))) AS avg_words_per_review
from 
  etsy-data-warehouse-prod.etsy_shard.shop_transaction_review
where 
  is_deleted = 0 --  only includes active reviews 

-- rating (yes/ no) 
-- have image (https://github.etsycorp.com/semanuele/projects/blob/master/Buying_Confidence/Reviews/ReviewsTopicModeling.sql)
with trans as (
select
	t.transaction_id
	,t.buyer_user_id
	,t.usd_subtotal_price
	,t.usd_price as item_price
	,t.quantity
	,t.listing_id
	,c.new_category as top_category
	,t.creation_tsz
from 
  etsy-data-warehouse-prod.transaction_mart.all_transactions t
join 
etsy-data-warehouse-prod.transaction_mart.all_transactions_categories c
  on t.transaction_id = c.transaction_id
  and t.listing_id = c.listing_id
where 
  extract(year from date(creation_tsz))>= 2022
)
, reviews as (
select
	t.*
	,p.buyer_segment
	,case when r.transaction_id is not null then 1 else 0 end as has_review
	,case when r.review is not null or r.review != '' then 1 else 0 end as has_text_review
	,case when i.transaction_id is not null then 1 else 0 end as has_image
	,rating
	,review
	,r.language
	-- ,timestamp(r.create_date) review_date
	-- ,min(timestamp(i.create_date)) first_image_date
	-- ,max(timestamp(i.create_date)) last_image_date
from 
  trans t
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_transaction_review r
    on t.buyer_user_id = r.buyer_user_id
    and t.transaction_id = r.transaction_id
left join 
  etsy-data-warehouse-prod.etsy_shard.user_appreciation_images i
    on t.transaction_id = i.transaction_id
    and t.buyer_user_id = i.buyer_user_id
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile p
    on t.buyer_user_id = p.mapped_user_id
group by all
)
select
  top_category,
  buyer_segment,
  has_review,
  has_text_review,
  has_image,
  count(distinct transaction_id) as transactions
from reviews
group by all


-- image on low stakes vs high stakes items 
