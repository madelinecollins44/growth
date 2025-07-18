/* SHOP REVIEW ON LISTING LEVEL */

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
, listing_shop_reviews as (
select
  case when b.total_reviews = 0 or b.seller_user_id is null then 1 else 0 end has_shop_reviews,
  a.listing_id,
  sum(purchased_after_view) as purchases,
  count(sequence_number) as views, 
from
  etsy-data-warehouse-prod.analytics.listing_views a
left join 
  shops_reviews b
    using (seller_user_id)
where  1=1
  and a._date >= current_date-30 
  and a.platform in ('mobile_web','desktop','boe')
group by all
)
select  has_shop_reviews, sum(views) from agg group by all 

  
  /* SIGNED IN VS SIGNED OUT CONVERSION */
with shops_wo_reviews as (
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
  l.platform,
  case when total_reviews = 0 or r.seller_user_id is null then 0 else 1 end as has_shop_reviews,
  -- case when transactions = 0 or r.seller_user_id is null then 0 else 1 end as has_transactions,
  -- case when user_id is null or user_id= 0 then 0 else 1 end as signed_in,
  -- seller_user_id,
  count(distinct l.listing_id) as viewed_listings, 
  sum(purchased_after_view) as purchases,
  count(sequence_number) as views, 
from 
  etsy-data-warehouse-prod.analytics.listing_views l
left join 
  shops_wo_reviews r using (seller_user_id)
inner join 
  etsy-data-warehouse-prod.weblog.visits v
      on l.visit_id=v.visit_id
where 
  l._date >= current_date-30 
  and v._date >= current_date-30 
  and l.platform in ('boe','mobile_web','desktop')
  -- and (total_reviews = 0 or r.seller_user_id is null) 
group by all 
order by 1,2,3,4 asc
