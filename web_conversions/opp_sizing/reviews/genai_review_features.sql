-----------------------------------------------------------------------------------------------
--looking at reviews x listing views/ gms coverage to understand what threshold we need to use
-----------------------------------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.genai_listings_opp_size as (
with gms as (
select
  listing_id,
  sum(trans_gms_net) as gms_net
from 
  etsy-data-warehouse-prod.transaction_mart.all_transactions a
left join 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans 
    using (transaction_id)
where a.date >= current_date - 365
group by all 
)
, listing_views as (
select
  a.listing_id,
  -- case
  -- 	when coalesce((p.price_usd/100), a.price_usd) > 100 then 'high stakes'
  -- 	else 'low stakes'
  -- end as listing_type,
  -- b.top_category,
  count(a.visit_id) as listing_views,
  sum(a.purchased_after_view) as purchases
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics b
left join 
  etsy-data-warehouse-prod.listing_mart.listings p using (listing_id)
left join  
    etsy-data-warehouse-prod.analytics.listing_views a
      on a.listing_id=b.listing_id
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
  -- listing_type,
  -- top_category,
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
);

--adding in top category + price 
create or replace table etsy-data-warehouse-dev.madelinecollins.genai_category_highstakes_listings_opp_size as (
with gms as (
select
  listing_id,
  sum(trans_gms_net) as gms_net
from 
  etsy-data-warehouse-prod.transaction_mart.all_transactions a
left join 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans 
    using (transaction_id)
where a.date >= current_date - 365
group by all 
)
, listing_views as (
select
  a.listing_id,
  case
  	when coalesce((p.price_usd/100), a.price_usd) > 100 then 'high stakes'
  	else 'low stakes'
  end as listing_type,
  b.top_category,
  count(a.visit_id) as listing_views,
  sum(a.purchased_after_view) as purchases
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics b
left join 
  etsy-data-warehouse-prod.listing_mart.listings p using (listing_id)
left join  
    etsy-data-warehouse-prod.analytics.listing_views a
      on a.listing_id=b.listing_id
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
  listing_type,
  top_category,
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
);


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

-------------------------------------------------------------------------------
-- testing
-------------------------------------------------------------------------------
--testing to make sure # of listings match with the query 
select
  count(distinct listing_id) as total_listings,
  count(lv.visit_id) as listing_views
from 
  etsy-data-warehouse-prod.analytics.listing_views lv -- only looking at viewed listings 
inner join 
  etsy-data-warehouse-prod.rollups.transaction_reviews using (listing_id)
where
  language in ('en')
  and lv._date >= current_date-30
having sum(has_review) >= 250
