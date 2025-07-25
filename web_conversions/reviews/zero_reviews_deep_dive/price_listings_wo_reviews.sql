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
  case when r.total_reviews = 0 or r.shop_id is null then 0 else 1 end has_shop_reviews,
  case 
    when coalesce(b.price_usd, v.price_usd) > 0 and coalesce(b.price_usd, v.price_usd) <= 10 then'$0-$10'
    when coalesce(b.price_usd, v.price_usd) > 10 and coalesce(b.price_usd, v.price_usd) <= 20 then'$10-$20'
    when coalesce(b.price_usd, v.price_usd) > 20 and coalesce(b.price_usd, v.price_usd) <= 40 then'$20-$40'
    when coalesce(b.price_usd, v.price_usd) > 40 and coalesce(b.price_usd, v.price_usd) <= 60 then'$40-$60'
    when coalesce(b.price_usd, v.price_usd) > 60 and coalesce(b.price_usd, v.price_usd) <= 80 then'$60-$80'
    when coalesce(b.price_usd, v.price_usd) > 80 and coalesce(b.price_usd, v.price_usd) <= 100 then'$80-$100'
    when coalesce(b.price_usd, v.price_usd) > 100 and coalesce(b.price_usd, v.price_usd) <= 120 then'$100-$120'
    when coalesce(b.price_usd, v.price_usd) > 120 and coalesce(b.price_usd, v.price_usd) <= 150 then'$120-$150'
    else 'over $150' 
  end as item_price_bucket,  
  count(distinct b.listing_id) as active_listings,
  sum(purchased_after_view) as purchases,
  count(sequence_number) as views, 
from
  etsy-data-warehouse-prod.rollups.active_listing_basics b
left join 
  etsy-data-warehouse-prod.analytics.listing_views v
      on v.listing_id=b.listing_id
left join 
  shops_reviews r
    on r.shop_id=b.shop_id
where  1=1
  and v._date >= current_date-30 
  and v.platform in ('mobile_web','desktop','boe')
  and (r.shop_id is null or r.total_reviews = 0) -- shop either has no transactions or no reviews
group by all
order by 1,2,3 desc
