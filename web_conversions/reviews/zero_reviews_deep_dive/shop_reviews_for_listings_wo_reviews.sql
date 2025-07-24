-- SHOP LEVEL
with shops_reviews as ( -- this looks at all listings that have been purchased and whether or not they have a review
select 
  shop_id,
  seller_user_id,
  count(distinct transaction_id) as transactions,
  sum(has_review) as total_reviews
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
group by all 
)
select
  case when r.total_reviews = 0 or r.shop_id is null then 0 else 1 end has_shop_reviews,
  count(distinct b.listing_id) as active_listings,
  sum(purchased_after_view) as purchases,
  count(sequence_number) as views, 
from
  etsy-data-warehouse-prod.rollups.active_listing_basics b
left join 
  etsy-data-warehouse-prod.analytics.listing_views a
      on a.listing_id=b.listing_id
left join 
  shops_reviews r
    on r.shop_id=b.shop_id
where  1=1
  and a._date >= current_date-30 
  and a.platform in ('mobile_web','desktop','boe')
group by all
order by 1,2,3 desc


-- LISTING LEVEL
with shops_reviews as ( -- this looks at all listings that have been purchased and whether or not they have a review
select 
  listing_id,
  count(distinct transaction_id) as transactions,
  sum(has_review) as total_reviews
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
group by all 
)
select
  case when r.total_reviews = 0 or r.listing_id is null then 0 else 1 end has_listing_reviews,
  count(distinct b.listing_id) as active_listings,
  sum(purchased_after_view) as purchases,
  count(sequence_number) as views, 
from
  etsy-data-warehouse-prod.rollups.active_listing_basics b
left join 
  etsy-data-warehouse-prod.analytics.listing_views a
      on a.listing_id=b.listing_id
left join 
  shops_reviews r
    on r.listing_id=b.listing_id
where  1=1
  and a._date >= current_date-30 
  and a.platform in ('mobile_web','desktop','boe')
group by all
order by 1,2,3 desc

-- LISTING AND SHOP LEVEL
with shops_reviews as ( -- this looks at all listings that have been purchased and whether or not they have a review
select 
  seller_user_id,
  count(distinct transaction_id) as transactions,
  sum(has_review) as total_reviews
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
group by all 
)
, listing_reviews as ( -- this looks at all listings that have been purchased and whether or not they have a review
select 
  listing_id,
  seller_user_id,
  count(distinct tr.transaction_id) as listing_transactions,
  sum(tr.has_review) as listing_reviews,
  sr.total_reviews as shop_reviews 
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews tr 
left join 
  shops_reviews sr
    using (seller_user_id)
group by all 
)
select
  platform,
  -- case when lr.listing_reviews = 0 or lr.listing_id is null then 0 else 1 end has_listing_reviews,
  -- case when lr.shop_reviews = 0 or lr.seller_user_id is null then 0 else 1 end has_shop_reviews,
  case 
    when lr.shop_reviews = 0 or lr.seller_user_id is null then 0 
    when lr.shop_reviews <= 10 then '1-10'
    when lr.shop_reviews <= 50 and lr.shop_reviews > 10 then '11-50'
    when lr.shop_reviews <= 5 then '1-5'
    else 1 end shop_reviews,
  count(distinct a.listing_id) as active_listings,
  sum(a.purchased_after_view) as purchases,
  count(a.sequence_number) as views, 
from
  etsy-data-warehouse-prod.analytics.listing_views a
left join 
  listing_reviews lr
    on lr.listing_id=a.listing_id
where  1=1
  and a._date >= current_date-30 
  and a.platform in ('mobile_web','desktop','boe')
  and (lr.listing_id is null or lr.listing_reviews = 0) -- listings without item reviews 
group by all
order by 1,2,3 desc


/* QUANTILES FOR # OF REVIEWS IN EACH SHOP*/
-- LISTING AND SHOP LEVEL
with shops_reviews as ( -- this looks at all listings that have been purchased and whether or not they have a review
select 
  seller_user_id,
  count(distinct transaction_id) as transactions,
  sum(has_review) as total_reviews
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
group by all 
)
, listing_reviews as ( -- this looks at all listings that have been purchased and whether or not they have a review
select 
  listing_id,
  seller_user_id,
  count(distinct tr.transaction_id) as listing_transactions,
  sum(tr.has_review) as listing_reviews,
  sr.total_reviews as shop_reviews 
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews tr 
left join 
  shops_reviews sr
    using (seller_user_id)
group by all 
)
, agg as (
SELECT
    lr.seller_user_id,
    shop_reviews,
    NTILE(4) OVER (ORDER BY shop_reviews) AS review_quartile
from
  etsy-data-warehouse-prod.analytics.listing_views a
inner join 
  listing_reviews lr
    on lr.listing_id=a.listing_id
where  1=1
  and a._date >= current_date-30 
  and a.platform in ('mobile_web','desktop')
  and (lr.listing_id is null or lr.listing_reviews = 0) -- listings without item reviews 
  -- and (lr.shop_reviews > 0 or lr.seller_user_id is not null)
group by all
)
SELECT
  review_quartile,
  MIN(shop_reviews) AS min_reviews_in_quartile,
  MAX(shop_reviews) AS max_reviews_in_quartile,
  COUNT(DISTINCT seller_user_id) as shops,
  AVG(shop_reviews) as avg_reviews
FROM
  agg
GROUP BY
  all 
