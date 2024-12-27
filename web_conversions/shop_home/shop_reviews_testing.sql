-- how many shops get visits? make sure this matches up with the final code is getting
with visit_shop_homes as (
select
   -- date(_partitiontime) as _date, 
  beacon.event_name, 
  (select value from unnest(beacon.properties.key_value) where key = "shop_id") as shop_id, 
  count(visit_id) as views, 
  count(distinct visit_id) as visits
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
  date(_partitiontime) >= current_date-14
  and beacon.event_source in ('web')
  and (beacon.event_name in ('shop_home'))
group by all
)
select count(distinct shop_id) from visit_shop_homes
---7080135

--testing shop review counts
with shop_reviews as (
select
  shop_id,
  count(distinct transaction_id) as transactions,
  count(distinct buyer_user_id) as buyers,
  avg(rating) as average_rating,
  count(case when rating = 0 then transaction_id end) as reviews_w_ratings_of_0,
  count(case when rating = 1 then transaction_id end) as reviews_w_ratings_of_1,
  count(case when rating = 2 then transaction_id end) as reviews_w_ratings_of_2,
  count(case when rating = 3 then transaction_id end) as reviews_w_ratings_of_3,
  count(case when rating = 4 then transaction_id end) as reviews_w_ratings_of_4,
  count(case when rating = 5 then transaction_id end) as reviews_w_ratings_of_5,
  count(case when seller_feedback != '' then transaction_id end) as reviews_w_seller_feedback
from 
  etsy-data-warehouse-prod.etsy_shard.shop_transaction_review
where 
  is_deleted = 0 --  only includes active reviews 
  and language in ('en') -- only english reviews
group by all
)
select
 sum(transactions) as total_reviews,
  avg(average_rating) as average_rating,
  -- sum(reviews_w_ratings_of_0) as reviews_w_ratings_of_0,
  sum(reviews_w_ratings_of_1) as reviews_w_ratings_of_1,
  sum(reviews_w_ratings_of_2) as reviews_w_ratings_of_2,
  sum(reviews_w_ratings_of_3) as reviews_w_ratings_of_3,
  sum(reviews_w_ratings_of_4) as reviews_w_ratings_of_4,
  sum(reviews_w_ratings_of_5) as reviews_w_ratings_of_5,
  sum(reviews_w_seller_feedback) as reviews_w_seller_feedback
from shop_reviews
