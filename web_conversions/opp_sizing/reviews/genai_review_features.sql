-----------------------------------------------------------------------------------------------
--looking at reviews x listing views/ gms coverage to understand what threshold we need to use
-----------------------------------------------------------------------------------------------
with gms as (
select
  listing_id,
  sum(trans_gms_net) as gms_net
from 
  etsy-data-warehouse-prod.transaction_mart.all_transactions
left join 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans 
    using (transaction_id)
group by all 
)
, listing_views as (
select
  listing_id,
  count(visit_id) as listing_views,
  sum(purchased_after_view) as purchases
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
group by all 
)
, reviews as (
select
  listing_id,
  count(transaction_id) as review_count
from  
  etsy-data-warehouse-prod.rollups.transaction_reviews
where 
  has_review > 0  
  and language in ('en')
group by all
order by 2 desc
)
select
  lv.listing_id,
  review_count as reviews,
  sum(listing_views) as listing_views,
  sum(purchases) as purchases,
  sum(gms_net) as gms_net,
from 
  listing_views lv
left join 
  reviews 
    on lv.listing_id=reviews.listing_id
left join 
  gms on lv.listing_id=gms.listing_id
group by all

-------------------------------------------------------------------------------
-- what does engagement + review distribution look like by top category? 
-------------------------------------------------------------------------------
-- active listings, high vs low stakes, listing views, gms coverage, unique visits, conversions, total reviews, review_seen events, review_seen visits, review distrbution by rating
with active_listing_views as (
select
  listing_id,
  price,
  views,
  unique_views,
  purchases,
  gms (?)
  )
, reviews as (
)
, reviews_seen 
