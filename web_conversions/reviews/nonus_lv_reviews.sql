---------------------------------------------------------------
-- OVERALL COUNTS TO CONFIRM 
---------------------------------------------------------------
-- total listing views
select
  count(sequence_number) as listing_views,
  sum(purchased_after_view) as purchases,
  count(distinct visit_id) as unique_visits
from  
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all
order by 2 desc 

-- total reviews 
select
  count(case when has_review > 0 then transaction_id end) as reviews
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews tr
group by all

-- % of visits 
select
    case 
    when detected_region in ('US') then 'US'
    else 'non-US'
    end as region,
  count(distinct visit_id)
from 
  etsy-data-warehouse-prod.weblog.visits
where 
  platform in ('mobile_web','desktop') 
  and _date >= current_date-30
group by all   
---------------------------------------------------------------
-- WHAT % OF LISTING VIEWS ARE FROM NON-US USERS? 
---------------------------------------------------------------
select
  detected_region,
  -- case 
  --   when detected_region in ('US') then 'US'
  --   else 'non-US'
  --   end as detected_region,
  count(sequence_number) as listing_views,
  sum(purchased_after_view) as purchases,
  count(distinct visit_id) as unique_visits
from  
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all
order by 2 desc 


---------------------------------------------------------------
-- WHAT % OF REVIEWS ARE LEFT BY NON-US BUYERS? 
---------------------------------------------------------------
select
  mup.country,
  count(case when has_review > 0 then transaction_id end) as reviews
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews tr
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile mup
    on mup.user_id=tr.buyer_user_id
group by all
order by 2 desc

-----------------------------------------------------------------------------
-- WHAT % OF VIEWED LISTINGS WILL HAVE REVIEWS FROM THE SAME REGION? 
-----------------------------------------------------------------------------
with views as (
select
  case 
    when detected_region in ('US') then 'US'
    else 'non-US'
    end as region,
  listing_id,
  count(sequence_number) as listing_views,
  sum(purchased_after_view) as purchases,
  count(distinct visit_id) as unique_visits
from  
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all
)
, reviews as (
select
  case 
    when mup.country in ('US') then 'US'
    else 'non-US'
  end as region,  
  listing_id,
  count(case when has_review > 0 then transaction_id end) as reviews
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews tr
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile mup
    on mup.user_id=tr.buyer_user_id
group by all 
)
-- of all non-US listing views, how many of them have non-US reviews
select
  region,
  count(distinct v.listing_id) as listings_viewed,
  count(distinct r.listing_id) as listings_reviewed,
  sum(listing_views) as all_listing_views,
  sum(purchases) as all_purchases,
  sum(reviews) as all_reviews,
  sum(case when r.listing_id is not null then listing_views end) as listing_views_for_listings_w_reviews,
  sum(case when r.listing_id is not null then purchases end) as purchases_for_listings_w_reviews
from 
  views v
left join 
  reviews r using (listing_id, region)
group by all 
