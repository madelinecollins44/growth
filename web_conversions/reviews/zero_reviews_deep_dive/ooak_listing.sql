/* LISTING LEVEL */

with listing_reviews as ( -- this looks at all listings that have been purchased and whether or not they have a review
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
  case when a.quantity = 1 and (total_gms = 0 or total_gms is null) then 1 else 0 end as ooak_listing,
  case when r.total_reviews = 0 or r.listing_id is null then 0 else 1 end has_listing_reviews,
  count(distinct a.listing_id) as active_listings, 
  count(v.listing_id) as viewed_listings,
  count(sequence_number) as views,
  sum(purchased_after_view) as purchases
from 
  `etsy-data-warehouse-prod.rollups.active_listing_basics` a
inner join 
 etsy-data-warehouse-prod.analytics.listing_views v 
    on a.listing_id=v.listing_id
left join 
  listing_reviews r
    on r.listing_id=a.listing_id
where 1=1
  and v._date >= current_date-30 
  and v.platform in ('mobile_web','desktop','boe')
group by all 
order by 1,2,3 desc


/* SHOP LEVEL */
-- with seller_count as (
select 
  a.shop_id,
  shop_name,
  count(distinct a.listing_id) as active_listings,
  count(distinct case when(mk.is_vintage != 1 and a.quantity > 1) then a.listing_id end) as non_vintage_listings,
  count(distinct case when(mk.is_vintage = 1 or a.quantity = 1) then a.listing_id end) as vintage_listing,
  case when ((count(distinct case when(mk.is_vintage = 1 or a.quantity = 1) then a.listing_id end)) / count(distinct a.listing_id)) > 0.5 then 1 else 0 end as majority_vintage
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics a
inner join 
  `etsy-data-warehouse-prod.materialized.listing_marketplaces` mk
    on a.listing_id = mk.listing_id
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics b
    on a.shop_id=b.shop_id
group by all
order by 2 desc
limit 10 
