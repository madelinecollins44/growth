-- TOP VIEWED LISTING
select listing_id, count(visit_id) 
from etsy-data-warehouse-prod.analytics.listing_views 
where _date >= current_date-30 
group by all order by 2 desc limit 5
-- listing_id	views
-- 1751473149	1688503
-- 1275734045	922209
-- 1768679396	448088
-- 539784195	354912
-- 1857643011	345707

-- TOP FAVORITED LISTING
select listing_id, count(*)
from etsy-data-warehouse-prod.etsy_shard.users_favoritelistings
where 
  is_displayable = 1
  and shop_id > 0 
  and date(timestamp_seconds(create_date)) >= current_date-30 
group by all order by 2 desc limit 5
-- listing_id	f0_
-- 1241115111	33497
-- 889161422	26910
-- 1463535135	20121
-- 1803392341	18200
-- 727008322	17992

-- TOP PURCHASED LISTING
select listing_id, count(transaction_id)
from etsy-data-warehouse-prod.transaction_mart.all_transactions
where date >= current_date-30 
group by all order by 2 desc limit 5
-- listing_id	f0_
-- 0	135063
-- 1839819193	6326
-- 1793293648	5836
-- 1651955178	4078
-- 1518307138	3721
