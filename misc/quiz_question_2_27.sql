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


-- TOP FAVORITED LISTING
select listing_id, count(*)
from etsy-data-warehouse-prod.etsy_shard.users_favoritelistings
where 
  is_displayable = 1
  and shop_id > 0 
  and date(timestamp_seconds(create_date)) >= current_date-30 
group by all order by 2 desc limit 5


-- TOP PURCHASED LISTING
select listing_id, count(transaction_id)
from etsy-data-warehouse-prod.transaction_mart.all_transactions
where date >= current_date-30 
group by all order by 2 desc limit 5
