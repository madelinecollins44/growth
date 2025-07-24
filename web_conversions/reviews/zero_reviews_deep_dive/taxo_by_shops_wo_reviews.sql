-- LISTING AND SHOP LEVEL
with shops_reviews as ( -- this looks at all listings that have been purchased and whether or not they have a review
select 
  shop_id,
  count(distinct transaction_id) as transactions,
  sum(has_review) as total_reviews
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
group by all 
)
, listing_reviews as ( -- this looks at all listings that have been purchased and whether or not they have a review
select 
  listing_id,
  shop_id,
  count(distinct tr.transaction_id) as listing_transactions,
  sum(tr.has_review) as listing_reviews,
  sr.total_reviews as shop_reviews 
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews tr 
left join 
  shops_reviews sr
    using (shop_id)
group by all 
)
select
  -- platform,
  case when r.total_reviews = 0 or r.shop_id is null then 0 else 1 end has_shop_reviews,
  top_category,
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
  and a.platform in ('mobile_web','desktop')
  and (r.shop_id is null or r.total_reviews = 0) -- shop either has no transactions or no reviews
group by all
order by 2,1 asc




-- what % of paper + party supply listings are digital 
select
  is_digital,
  count(distinct a.listing_id) as listings,
  -- case when a.quantity = 1 and (total_gms = 0 or total_gms is null) then 1 else 0 end as ooak_listing,
  -- case when r.total_reviews = 0 or r.listing_id is null then 0 else 1 end has_listing_reviews,
  -- count(distinct a.listing_id) as active_listings, 
  -- count(listing_id) as viewed_listings,
  count(sequence_number) as views,
  sum(purchased_after_view) as purchases
from 
  `etsy-data-warehouse-prod.rollups.active_listing_basics` a
inner join 
 etsy-data-warehouse-prod.analytics.listing_views v 
    on a.listing_id=v.listing_id
where 1=1 
  and top_category in ('paper_and_party_supplies')
  and v._date >= current_date-30 
  and v.platform in ('mobile_web','desktop','boe') 
group by all 
