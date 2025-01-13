----------------------------------------------------------------------------------------------------
-- HOW MANY SELLERS RESPONSE TO REVIEWS? 
----Is there a difference between low stake vs high stake items, or seller type?
----------------------------------------------------------------------------------------------------
select
  seller_tier_new,
  count(distinct case when active_seller_status = 1 then shop_id end) as active_shops,
  count(distinct r.shop_id) as shops_w_responses,
  count(distinct case when active_seller_status = 1 then r.shop_id end) as active_shops_w_responses 
from 
  etsy-data-warehouse-prod.rollups.seller_basics sb
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_transaction_review_response r using (shop_id)
group by all 

-- select count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_transaction_review_response
--  -- 1158469

--  select count(distinct shop_id) from etsy-data-warehouse-prod.rollups.seller_basics where active_seller_status = 1
-- -- 5668410

-----how many transactions have replies 
select
  count(distinct tr.transaction_id) as transactions_w_reviews,
  count(distinct r.transaction_id) as trans_w_responses,
  count(distinct case when r.transaction_id is null then tr.transaction_id end) as trans_wo_responses
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews tr
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_transaction_review_response r using (transaction_id)
where tr.has_review=1 

--high vs low stakes
with all_trans as (
select
 case 
    when item_price > 100 then 'high stakes' 
    else 'low stakes' 
  end as item_type,
  transaction_id
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
where has_review > 0
group by all)
select
  item_type,
  count(distinct trans.transaction_id) as trans_w_reviews,
  count(distinct tr.transaction_id) as trans_w_response
from 
  all_trans trans
left join
  etsy-data-warehouse-prod.etsy_shard.shop_transaction_review_response tr using (transaction_id)
group by all
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

--review attributes
select
  count(distinct transaction_id) as transactions,
  count(distinct case when has_review=1 then transaction_id end) as reviews,
  count(distinct case when has_image=1 then transaction_id end) as images,
  count(distinct case when has_video=1 then transaction_id end) as videos,
  count(distinct case when has_subrating=1 then transaction_id end) as subratings,
  count(case when rating = 1 then transaction_id end) as reviews_w_ratings_of_1,
  count(case when rating = 2 then transaction_id end) as reviews_w_ratings_of_2,
  count(case when rating = 3 then transaction_id end) as reviews_w_ratings_of_3,
  count(case when rating = 4 then transaction_id end) as reviews_w_ratings_of_4,
  count(case when rating = 5 then transaction_id end) as reviews_w_ratings_of_5,
  avg(rating) as avg_rating
from
  etsy-data-warehouse-prod.rollups.transaction_reviews

--% of reviews w a seller response
select 
  count(distinct case when has_review = 1 then a.transaction_id end) as trans_w_reviews,
  count(distinct case when is_deleted != 1 then b.transaction_id end) as reviews_w_seller_response,
  count(distinct case when is_deleted != 1 then b.transaction_id end)/count(distinct case when has_review = 1 then a.transaction_id end) as share_w_response
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews a
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_transaction_review_response b using (transaction_id)
group by all

--------------------------------------------------
-- SHOP HOME VS LISTING PAGE REVIEW COMPARISON
--------------------------------------------------
select 
  count(distinct v.visit_id) as total_visits,
  count(distinct case when event_type in ('view_listing') then v.visit_id end) as listing_page,
  count(distinct case when event_type in ('shop_home') then v.visit_id end) as shop_home,
  count(distinct case when event_type in ('reviews_anchor_click') and url like ('%listing%') then v.visit_id end) as listing_page_review_jumps,
  count(distinct case when event_type in ('shop_home_reviews_jump_link_click') then v.visit_id end) as shop_home_review_jumps,
  count(distinct case when event_type in ('listing_page_reviews_pagination') then v.visit_id end) as listing_page_pagination,
  count(distinct case when event_type in ('shop_home_reviews_pagination') then v.visit_id end) as shop_home_pagination,
  count(distinct case when event_type in ('listing_page_reviews_pagination') then v.visit_id end) as listing_page_pagination,
  count(distinct case when event_type in ('listing_page_reviews_seen') then v.visit_id end) as listing_page_reviews_seen,
  count(distinct case when event_type in ('shop_home_reviews_section_seen') then v.visit_id end) as shop_home_reviews_seen,

from 
  etsy-data-warehouse-prod.weblog.events e
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date-14
  and platform in ('mobile_web','desktop')

-----------------------------------------------------------------------------------------------------------------------
-- LISTING COMPARISON
---- What % of listings and listing views have photos, videos, seller responses and is there a CR difference/impact?
-----------------------------------------------------------------------------------------------------------------------
with review_attributes as (
select
  listing_id,
  count(distinct transaction_id) as transactions,
  sum(has_review) as has_review,
  sum(has_image) as has_image,
  sum(has_video) as has_video,
  sum(has_subrating) as has_subrating,
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
where
  active_listing = 1 -- only looking at active listings
group by all
)
, seller_responses as (
select 
  b.listing_id,
  count(distinct a.transaction_id) as has_seller_response, -- transactions are duped bc of multiple responses, so only looking to see if transaction is in this table 
from 
  etsy-data-warehouse-prod.etsy_shard.shop_transaction_review_response a
inner join 
  etsy-data-warehouse-prod.rollups.transaction_reviews b using (transaction_id)
where
  a.is_deleted != 1 -- only live responses
group by all
)
, listing_views as (
select
  listing_id,
  count(visit_id) as views,
  sum(purchased_after_view) as purchases
from 
  etsy-data-warehouse-prod.analytics.listing_views
where
  _date >= current_date-30
--and platform in ('mobile_web','desktop') -- when looking at web only
group by all
)
select
  -- case when has_review > 0 then 1 else 0 end as has_review,
  -- case when has_image > 0 then 1 else 0 end as has_image,
  -- case when has_video > 0 then 1 else 0 end as has_video,
  -- case when has_subrating > 0 then 1 else 0 end as has_subrating,
  case when has_seller_response > 0 then 1 else 0 end as has_seller_responses,
  count(distinct a.listing_id) as active_listings,
  count(distinct v.listing_id) as viewed_listings,
  sum(views) as listing_views,
  sum(purchases) as purchases,
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics a
left join 
  listing_views v using (listing_id)
left join 
  seller_responses sr 
    on a.listing_id=sr.listing_id
left join 
  review_attributes ra
    on a.listing_id= ra.listing_id
group by all 
	
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
  and language in ('en') -- only english reviews

----------------VERSION 1: looks at attributes across reviews only 
with listing_attributes as (
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
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews tr
left join 
  listing_attributes la 
    on tr.listing_id = la.listing_id
where 
  tr.active_listing = 1 -- only reviews of active listings 
group by all

	
----------------VERSION 2: looks at review attributes across all transactions 
-- have image (https://github.etsycorp.com/semanuele/projects/blob/master/Buying_Confidence/Reviews/ReviewsTopicModeling.sql) -- all languages
-- start with all purchases since 2022
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
	all_trans.*
	, reviews.has_review 
  , reviews.has_text_review 
  , reviews.has_image
	, reviews.has_video
	, reviews.rating
	, reviews.review
from 
  trans all_trans
left join 
  etsy-data-warehouse-prod.rollups.transaction_reviews reviews 
    on all_trans.buyer_user_id = reviews.buyer_user_id
    and all_trans.transaction_id = reviews.transaction_id
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_transaction_review seller_feedback
    on all_trans.buyer_user_id = seller_feedback.buyer_user_id
    and all_trans.transaction_id = seller_feedback.transaction_id
group by all
)
select
  case when item_price > 100 then 'high stakes' else 'low stakes' end as item_type,
  count(distinct transaction_id) as total_transactions, -- since 2022
  has_review,
  has_text_review,
  has_image,
  has_video,
from 
  reviews
group by all


--verison 2 testing 
-- start with all purchases since 2022
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
	all_trans.*
	, reviews.has_review 
  , reviews.has_text_review 
  , reviews.has_image
	, reviews.has_video
  , case when seller_feedback.seller_feedback != " " or seller_feedback.seller_feedback is not null then 1 else 0 end as has_seller_feedback
	, reviews.rating
	, reviews.review
from 
  trans all_trans
left join 
  etsy-data-warehouse-prod.rollups.transaction_reviews reviews 
    on all_trans.buyer_user_id = reviews.buyer_user_id
    and all_trans.transaction_id = reviews.transaction_id
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_transaction_review seller_feedback
    on all_trans.buyer_user_id = seller_feedback.buyer_user_id
    and all_trans.transaction_id = seller_feedback.transaction_id
group by all
)
-- select * from reviews where has_review = 1 and has_image = 1 limit 5 
--trans, listing
-- 4324092317, 1111321190
-- 4289626124, 1774608492
-- 4081967467, 1513560570
-- 4414630046, 554867876
----testing to be sure lisitngs dont look the same depending on trans 
-- 6 trans, 1 review, 1 image, 5 rating, $6
select
  case when item_price > 100 then 'high stakes' else 'low stakes' end as item_type,
  sum(has_review) as has_review,
  sum(has_text_review) as has_text_review,
  sum(has_image) as has_image,
  sum(has_video) as has_video,
  sum(has_seller_feedback) as has_seller_feedback,
  count(distinct transaction_id) as transactions
from reviews
where listing_id = 1774608492
group by all
