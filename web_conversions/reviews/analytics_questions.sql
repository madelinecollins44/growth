--------------------------------------------------
-- HOW MANY WORDS IS A REVIEW ON AVERAGE
--------------------------------------------------
with word_count as (
select 
  transaction_id, 
  review, 
  ((LENGTH(review) - LENGTH(replace(review, ' ', ''))) + 1) as review_length
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
where 
  has_text_review = 1 -- review must include text
  and language in ('en') -- in english
)
select
  count(distinct transaction_id) as transactions,
  sum(review_length) as total_words_used,
  avg(review_length) as avg_words_used
from 
  word_count

-- most popular word in reviews
with word_count AS (
select 
  lower(word) AS word,
  count(*) AS word_frequency,
  count(distinct transaction_id) AS unique_transactions,
from 
    etsy-data-warehouse-prod.rollups.transaction_reviews,
unnest(split(regexp_replace(review, r'[^\w\s]', ''), ' ')) AS word
group by all 
)
select * from word_count 
order by 2 desc 
limit 100

--------------------------------------------------
--BREAKDOWN OF OPTIONAL FIELDS 
--------------------------------------------------
-- yes/ no to recommending item 
select
   -- date(_partitiontime) as _date, 
  beacon.event_name, 
  (select value from unnest(beacon.properties.key_value) where key = "value"),
  count(visit_id) as views, 
  count(distinct visit_id) as visits
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
  date(_partitiontime) >= current_date-14
  and beacon.event_source in ('web')
  and (beacon.event_name in ('review_submission_is_recommended_submitted'))
-- looking at favoriting on shop_home page
group by all
	
--------------------------------------------------
-- SHOP HOME VS LISTING PAGE REVIEW COMPARISON
--------------------------------------------------
-- click stars to access review component, pagination 
------- listing page event; reviews_anchor_click, loc has 'listing'
------- shop home event; shop_home_reviews_jump_link_click
------- listing page event; listing_page_reviews_pagination
------- shop home event; shop_home_reviews_pagination
	
select 
  count(distinct v.visit_id) as total_visits,
  count(distinct case when event_type in ('view_listing') then v.visit_id end) as listing_page,
  count(distinct case when event_type in ('shop_home') then v.visit_id end) as shop_home,
  count(distinct case when event_type in ('reviews_anchor_click') and url like ('%listing%') then v.visit_id end) as listing_page_review_jumps,
  count(distinct case when event_type in ('shop_home_reviews_jump_link_click') then v.visit_id end) as shop_home_review_jumps,
  count(distinct case when event_type in ('listing_page_reviews_pagination') then v.visit_id end) as listing_page_pagination,
  count(distinct case when event_type in ('shop_home_reviews_pagination') then v.visit_id end) as shop_home_pagination,
from 
  etsy-data-warehouse-prod.weblog.events e
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date-14
  and platform in ('mobile_web','desktop')

-- pagination 
------- listing page event; listing_page_reviews_pagination
------- shop home event; shop_home_reviews_pagination

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
----------------VERSION 1 
--HOW MANY REVIEWS HAVE SELLER FEEDBACK, IMAGES, VIDEOS
-- start with transactions w seller feedback 
with seller_feedback as (
select
  transaction_id,
  case when seller_feedback != " " or seller_feedback is not null then 1 else 0 end as has_seller_feedback
from 
  etsy-data-warehouse-prod.etsy_shard.shop_transaction_review
where
  is_deleted = 0 -- only active reviews 
)
--get high stakes vs low stakes listings 
, listing_attributes as (
select
  listing_id,
  case when price_usd > 100 then 'high stakes' else 'low stakes' end as item_type
from
	etsy-data-warehouse-prod.rollups.active_listing_basics
)
select
  la.item_type,
  sum(has_review) as reviews,
  sum(has_image) as image,
  sum(has_video) as has_video,
  sum(has_seller_feedback) as has_seller_feedback,
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews tr
left join 
  seller_feedback sf using (transaction_id)
left join 
  listing_attributes la 
    on tr.listing_id = la.listing_id
where 
  tr.active_listing = 1 -- only reviews of active listings 
group by all



----------------VERSION 2 
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
  and language in ('en') -- only english reviews

-- have image (https://github.etsycorp.com/semanuele/projects/blob/master/Buying_Confidence/Reviews/ReviewsTopicModeling.sql) -- all languages
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
  ,case when r.seller_feedback != " " or r.seller_feedback is not null then 1 else 0 end as has_seller_feedback
	,case when r.review is not null or r.review != '' then 1 else 0 end as has_text_review
	,case when i.transaction_id is not null then 1 else 0 end as has_image
  ,case when v.transaction_id is not null then 1 else 0 end as has_video
	,rating
	,review
	-- ,r.language
	-- ,timestamp(r.create_date) review_date
	-- ,min(timestamp(i.create_date)) first_image_date
	-- ,max(timestamp(i.create_date)) last_image_date
from 
  trans t
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_transaction_review r
    on t.buyer_user_id = r.buyer_user_id
    and t.transaction_id = r.transaction_id
    -- and r.language in ('en')
left join 
  etsy-data-warehouse-prod.etsy_shard.user_appreciation_images i
    on t.transaction_id = i.transaction_id
    and t.buyer_user_id = i.buyer_user_id
left join 
  etsy-data-warehouse-prod.etsy_shard.user_appreciation_videos v
    on t.transaction_id = v.transaction_id
    and t.buyer_user_id = v.buyer_user_id
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile p
    on t.buyer_user_id = p.mapped_user_id
-- where 
--   r.language in ('en')
group by all
)
select
  -- top_category,
  -- buyer_segment,
  case when item_price > 100 then 'high stakes' else 'low stakes' end as item_type,
  has_review,
  has_text_review,
  has_image,
  has_video,
  has_seller_feedback,
  count(distinct transaction_id) as transactions
from reviews
group by all
