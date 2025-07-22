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
