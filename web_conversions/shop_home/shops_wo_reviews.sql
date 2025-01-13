--how many pageviews do shops without reviews have ? 
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
, shop_reviews as (
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
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
where 
  has_review = 1 
  and language in ('en') -- only english reviews
group by all
)
select
  count(distinct v.shop_id) as unique_shops_visited,
  case 
    when sum(views) > 50 then '50'
    when sum(views) > 100 then '100'
    when sum(views) > 200 then '200'
    when sum(views) > 300 then '300'
    when sum(views) > 400 then '400'
    when sum(views) > 500 then '500'
    else '500+'
  end as shop_home_pageviews,
from
  visit_shop_homes v
left join 
  shop_reviews r 
    on v.shop_id=cast(r.shop_id as string)
where 
  r.shop_id is null -- only looking at shops without reviews

-- unique_shops_visited	shop_home_pageviews
-- 8276717	50

--increasing pageview threshold to change review distribution 
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
, shop_reviews as (
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
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
where 
  has_review = 1 
  -- and language in ('en') -- only english reviews
group by all
)
select
  count(distinct v.shop_id) as unique_shops_visited,
  sum(views) as shop_home_pageviews,
  count(distinct r.shop_id) as shop_w_reviews,
  sum(case when r.shop_id is null then 1 else 0 end) as shops_without_reviews,
  sum(transactions) as total_reviews,
  avg(transactions) as avg_reviews,
  avg(average_rating) as average_rating,
  -- sum(reviews_w_ratings_of_0) as reviews_w_ratings_of_0,
  sum(reviews_w_ratings_of_1) as reviews_w_ratings_of_1,
  sum(reviews_w_ratings_of_2) as reviews_w_ratings_of_2,
  sum(reviews_w_ratings_of_3) as reviews_w_ratings_of_3,
  sum(reviews_w_ratings_of_4) as reviews_w_ratings_of_4,
  sum(reviews_w_ratings_of_5) as reviews_w_ratings_of_5,
  -- avg(reviews_w_ratings_of_0) as avg_reviews_w_ratings_of_0,
  avg(reviews_w_ratings_of_1) as avg_reviews_w_ratings_of_1,
  avg(reviews_w_ratings_of_2) as avg_reviews_w_ratings_of_2,
  avg(reviews_w_ratings_of_3) as avg_reviews_w_ratings_of_3,
  avg(reviews_w_ratings_of_4) as avg_reviews_w_ratings_of_4,
  avg(reviews_w_ratings_of_5) as avg_reviews_w_ratings_of_5,
from 
  visit_shop_homes v
left join 
  shop_reviews r 
    on v.shop_id=cast(r.shop_id as string)
where v.views >= 100 -- only shops with at least 50 views

---only 2.7% of all shop homes have reviews 
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
-- select count(distinct shop_id) from visit_shop_homes
--8489264
, shop_reviews as (
select
  shop_id,
  count(distinct transaction_id) as transactions,
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
where 
  has_review = 1 
  -- and language in ('en') -- only english reviews
group by all
)
select
  count(distinct v.shop_id) as visited_shops,
  count(distinct r.shop_id) as reviewed_shops
from 
  visit_shop_homes v
inner join 
  shop_reviews r 
    on v.shop_id=cast(r.shop_id as string)
group by all
--230610

select 230610/8489264
--0.027164899100793661
