----------------------------------------------------------------------------------------
-- OVERALL REVIEWS DISTROS
----------------------------------------------------------------------------------------
select
  -- listing_id,
  -- case when date(transaction_date) >= current_date-365 then 1 else 0 end as reviews_in_last_year,
  -- case when date(transaction_date) < current_date-365 then 1 else 0 end as reviews_before_last_year,
  count(distinct transaction_id) as total_reviews,
  count(distinct case when date(transaction_date) >= current_date-365 then transaction_id end) reviews_in_last_year,
  count(distinct case when date(transaction_date) < current_date-365 then transaction_id end) reviews_in_before_last_year
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews  
where 
  has_review > 0 -- only listings 
group by all

select
  extract(year from date(transaction_date)) as year,
  count(distinct transaction_id) as total_reviews,
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews  
where 
  has_review > 0 -- only listings 
group by all
order by 1 desc
  
----------------------------------------------------------------------------------------
-- LISTING VIEWS + ACTIVE LISTINGS BY REVIEW TIME
----------------------------------------------------------------------------------------
with listing_reviews as (
select
  listing_id,
  -- case when date(transaction_date) >= current_date-365 then 1 else 0 end as reviews_in_last_year,
  -- case when date(transaction_date) < current_date-365 then 1 else 0 end as reviews_before_last_year,
  count(distinct transaction_id) as total_reviews,
  count(distinct case when date(transaction_date) >= current_date-365 then transaction_id end) reviews_in_last_year,
  count(distinct case when date(transaction_date) < current_date-365 then transaction_id end) reviews_in_before_last_year
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews  
where 
  has_review > 0 -- only listings 
group by all
)
, listing_views as (
select
  platform,
  listing_id,
  count(sequence_number) as total_views
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  and platform in ('desktop','mobile_web','boe')
group by all
)
select
  case 
    when reviews_in_last_year > 0 and reviews_in_before_last_year = 0 then 'only_reviews_this_year'
    when reviews_in_last_year = 0 and reviews_in_before_last_year > 0 then 'only_reviews_before_this_year'
    when reviews_in_last_year > 0 and reviews_in_before_last_year > 0 then 'reviews_in_both'
    else 'error'
  end as reviews_type,
  count(distinct a.listing_id) as active_listings,
  count(distinct v.listing_id) as viewed_listings,
  count(distinct case when platform in ('desktop') then v.listing_id end) as desktop_listings,
  count(distinct case when platform in ('mobile_web') then v.listing_id end) as mweb_listing,
  count(distinct case when platform in ('boe') then v.listing_id end) as boe_listing,
  -- views
  sum(total_views) as listing_views,
  sum(case when platform in ('desktop') then total_views end) as desktop_views,
  sum(case when platform in ('mobile_web') then total_views end) as mweb_views,
  sum(case when platform in ('boe') then total_views end) as boe_views
from
  etsy-data-warehouse-prod.rollups.active_listing_basics a
left join 
  listing_views v using (listing_id)
left join 
  listing_reviews r
    on a.listing_id=r.listing_id
group by all 

-- );

-- end
  
--------------------------------------------------------------------------------------------------------
-- TESTING
--------------------------------------------------------------------------------------------------------
select
  listing_id,
  count(distinct transaction_id) as total_reviews,
  count(distinct case when date(transaction_date) >= current_date-365 then transaction_id end) reviews_in_last_year,
  count(distinct case when date(transaction_date) <= current_date-365 then transaction_id end) reviews_in_before_last_year
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews  
where 
  has_review > 0 -- only listings 
group by all 
having count(distinct transaction_id) >= 20
order by 2 desc limit 5

/* 
listing_id	reviews_in_last_year	reviews_in_before_last_year
273159520	0	1
229808336	0	1
1451134532	0	2
1541322607	0	1
576911234	0	1
1518307138	8927	2042
1167562350	7205	18701
1413455244	5941	11622
1678173188	5796	1805
1651955178	5188	527
*/
