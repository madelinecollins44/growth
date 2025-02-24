/* TOP VIEWED LISTING*/ 
-- top viewed listing
select listing_id, count(visit_id) 
from etsy-data-warehouse-prod.analytics.listing_views 
inner join etsy-data-warehouse-prod.rollups.active_listing_basics using (listing_id)
where _date >= current_date-30 
group by all order by 2 desc limit 5

-- top 500 listings, randomized
with active_listing_views as (
select 
  listing_id, 
  count(visit_id) 
from 
  etsy-data-warehouse-prod.analytics.listing_views 
inner join 
  etsy-data-warehouse-prod.rollups.active_listing_basics using (listing_id)
where 
  _date >= current_date-30 
group by all 
order by 2 desc limit 500
)
select * from active_listing_views order by rand() limit 10


/* TOP FAVORITES LISTING*/ 
-- top favorited listing
select listing_id, count(*)
from etsy-data-warehouse-prod.etsy_shard.users_favoritelistings f
inner join etsy-data-warehouse-prod.rollups.active_listing_basics using (listing_id)
where is_displayable = 1
  and f.shop_id > 0 
  and date(timestamp_seconds(f.create_date)) >= current_date-30 -- favorited in last 30 days
group by all order by 2 desc limit 5

-- top 500 listings, randomized
 with active_favorited_listings as (
select listing_id, count(*)
from etsy-data-warehouse-prod.etsy_shard.users_favoritelistings f
inner join etsy-data-warehouse-prod.rollups.active_listing_basics using (listing_id)where 
  is_displayable = 1
  and f.shop_id > 0 
  and date(timestamp_seconds(f.create_date)) >= current_date-30 
group by all order by 2 desc limit 500
)
select * from active_favorited_listings order by rand() limit 10

/* TOP PURCHASED LISTING*/ 
-- top favorited listing
select listing_id, count(transaction_id)
from etsy-data-warehouse-prod.transaction_mart.all_transactions
inner join etsy-data-warehouse-prod.rollups.active_listing_basics using (listing_id)
where date >= current_date-30  and listing_id > 0 
group by all order by 2 desc limit 5

-- top 500 listings, randomized
with active_purchased_listings as (
select listing_id, count(transaction_id)
from etsy-data-warehouse-prod.transaction_mart.all_transactions
inner join etsy-data-warehouse-prod.rollups.active_listing_basics using (listing_id)
where date >= current_date-30  and listing_id > 0 
group by all order by 2 desc limit 500
)
select * from active_purchased_listings order by rand() limit 10
