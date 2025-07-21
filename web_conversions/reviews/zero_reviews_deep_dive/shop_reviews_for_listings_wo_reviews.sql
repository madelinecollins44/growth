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
, listing_reviews as (
select 
  listing_id,
  count(distinct transaction_id) as transactions,
  sum(has_review) as total_reviews
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
group by all 
)
select
  platform,

  count(distinct a.listing_id) as listings,
  sum(purchased_after_view) as purchases,
  count(sequence_number) as views, 
from
  etsy-data-warehouse-prod.analytics.listing_views a
inner join 
  listing_reviews l
    on a.listing_id=l.listing_id
left join 
  shops_reviews s 
   on s.seller_user_id=a.seller_user_id
where  1=1
  and a._date >= current_date-30 
  and a.platform in ('mobile_web','desktop','boe')
  and s.total_reviews= 0 -- shops without any reviews 
group by all
order by 1,2,3 desc
