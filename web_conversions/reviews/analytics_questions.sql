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
-- word count
etsy-data-warehouse-prod.etsy_shard.shop_transaction_review

-- rating (yes/ no) 

-- seller feedback
  use seller_feedback from etsy-data-warehouse-prod.etsy_shard.shop_transaction_review
  
-- have image 
select
	t.*
	,p.buyer_segment
	,case when r.transaction_id is not null then 1 else 0 end as has_review
	,case when r.review is not null or r.review != '' then 1 else 0 end as has_text_review
	,case when i.transaction_id is not null then 1 else 0 end as has_image
	,rating
	,review
	,r.language
	,to_timestamp(r.create_date) review_date
	,min(to_timestamp(i.create_date)) first_image_date
	,max(to_timestamp(i.create_date)) last_image_date
from trans t
left join etsy_shard.shop_transaction_review r
on t.buyer_user_id = r.buyer_user_id
and t.transaction_id = r.transaction_id
left join etsy_shard.user_appreciation_images 

-- image on low stakes vs high stakes items 
