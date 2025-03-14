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

