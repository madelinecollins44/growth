with listing_rating as (
select
  listing_id,
  case 
    when coalesce(avg(rating),0) = 0 then '0'
    when coalesce(avg(rating),0) > 0 and coalesce(avg(rating),0) <= 1 then '1'
    when coalesce(avg(rating),0) > 1 and coalesce(avg(rating),0)<= 2 then '2'
    when coalesce(avg(rating),0) > 2 and coalesce(avg(rating),0)<= 3 then '3'
    when coalesce(avg(rating),0) > 3 and coalesce(avg(rating),0) <= 4 then '4'
    when coalesce(avg(rating),0) > 4 and coalesce(avg(rating),0) <= 5 then '5'
    else 'error'
  end as avg_rating
from
  `etsy-data-warehouse-prod.rollups.transaction_reviews` 
where 
  transaction_date >= timestamp_sub(current_timestamp(), interval 365 DAY)
group by all 
)
, listing_views as (
select
  listing_id, 
  count(sequence_number) as views
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  platform in ('mobile_web','desktop')
and 
  _date >= current_date-30
group by all
)
select
  avg_rating,
  count(distinct listing_id) as unique_listings,
  sum(views) as total_lv
from 
  listing_views
left join 
  listing_rating using (listing_id)
group by all 
order by 1 asc
