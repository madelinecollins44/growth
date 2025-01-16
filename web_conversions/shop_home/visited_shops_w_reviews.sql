-- find shops with most pageviews, gms coverage, and no reviews
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
  count(distinct case when has_review =1 then transaction_id end) as reviews,
  -- count(distinct buyer_user_id) as buyers,
  avg(rating) as average_rating,
  count(case when rating = 0 then transaction_id end) as reviews_w_ratings_of_0,
  count(case when rating = 1 then transaction_id end) as reviews_w_ratings_of_1,
  count(case when rating = 2 then transaction_id end) as reviews_w_ratings_of_2,
  count(case when rating = 3 then transaction_id end) as reviews_w_ratings_of_3,
  count(case when rating = 4 then transaction_id end) as reviews_w_ratings_of_4,
  count(case when rating = 5 then transaction_id end) as reviews_w_ratings_of_5,
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
-- where 
  -- and language in ('en') -- only english reviews
group by all
)
, total_gms as (
select
  b.shop_id, 
  shop_name,
  sum(gms_net) as gms_net
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms gms
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics b 
    on gms.seller_user_id=b.user_id
where trans_date >= current_date-90
group by all 
)
select
  v.shop_id,
  gms.shop_name,
  sum(views) as total_views,
  gms_net
from 
  visit_shop_homes v
left join 
  shop_reviews r 
    on v.shop_id=cast(r.shop_id as string)
left join 
  total_gms gms
    on v.shop_id=cast(gms.shop_id as string)
where reviews=0
group by all
order by 3 desc limit 5
	
