with review_count as (
select
    review, 
    listing_id, 
    count(*) AS cnt
  from 
    `etsy-data-warehouse-prod.etsy_shard.shop_transaction_review`  
  where
      date(timestamp_seconds(create_date)) 
      and review <> ''
      and review is not null 
  group by all 
  having count(*) > 1
) 
, duped_listings as (
select distinct
  listing_id
from 
  review_count
)
, lv_stats as (
select
  listing_id,
  count(sequence_number) as views,
  sum(purchased_after_view) as purchases 
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  and platform in ('mobile_web','desktop')
)
select
  count(distinct lvs.listing_id) as viewed_listings,
  count(distinct dl.listing_id) as duped_viewed_listings,
  sum(views) as total_lv,
  sum(case when dl.listing_id is not null then views end) as duped_lv,
  sum(purchases) as total_purchases,
  sum(case when dl.listing_id is not null then purchases end) as duped_purchases
from 
  lv_stats lvs
left join 
  duped_listings dl using (listing_id)
  
