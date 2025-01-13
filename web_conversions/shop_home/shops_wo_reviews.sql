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
