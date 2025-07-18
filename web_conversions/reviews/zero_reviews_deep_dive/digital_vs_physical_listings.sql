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
  platform,
  case when b.total_reviews > 0 or b.seller_user_id is null then 1 else 0 end has_shop_reviews,
  is_digital,
  count(distinct a.listing_id) as listings,
  sum(purchased_after_view) as purchases,
  count(sequence_number) as views, 
from
  etsy-data-warehouse-prod.analytics.listing_views a
left join 
  shops_reviews b
    using (seller_user_id)
inner join 
  etsy-data-warehouse-prod.listing_mart.listing_attributes atr 
    on a.listing_id=atr.listing_id
where  1=1
  and a._date >= current_date-30 
  and a.platform in ('mobile_web','desktop','boe')
group by all
order by 1,2,3 desc
