------------------------------------------------------------------------------------------
-- PRICE DISTRO OF LISTINGS IN VISITED SHOPS 
------------------------------------------------------------------------------------------
-- create or replace table etsy-data-warehouse-dev.madelinecollins.web_shop_visits as (
-- select
--   platform,
--   beacon.event_name, 
--   (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id, 
--   (select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id, 
--   visit_id, 
--   sequence_number,
-- from
--   `etsy-visit-pipe-prod.canonical.visit_id_beacons`
-- inner join 
--   etsy-data-warehouse-prod.weblog.visits using (visit_id)
-- where
--   date(_partitiontime) >= current_date-30
--   and _date >= current_date-30
--   and platform in ('mobile_web','desktop','boe')
--   and (beacon.event_name in ('shop_home'))
-- group by all
-- );


-- get price distros for all visited shops 
select
  case 
   when a.price_usd < 1 then 'Less than $1'
   when a.price_usd >= 1 and a.price_usd < 5 then '$1-$4.99'
   when a.price_usd >= 5 and a.price_usd < 10 then '$5-$9.99'
   when a.price_usd >= 10 and a.price_usd < 25 then '$10-$24.99'
   when a.price_usd >= 25 and a.price_usd < 50 then '$25-$49.99'
   when a.price_usd >= 50 and a.price_usd < 75 then '$50-$74.99'
   when a.price_usd >= 75 and a.price_usd < 100 then '$75-$99.99'
  else '100+'
  end as price_buckets,
	count(a.listing_id) as active_listings,
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics a
inner join 
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits v -- only looking at listings from visited shops
    on cast(a.shop_id as string)=v.shop_id
group by all

------------------------------------------------------------------------------------------
-- NUMBER OF ACTIVE LISTINGS PER VISITED SHOP, GMS COVERAGE 
------------------------------------------------------------------------------------------
with visited_shops as (
select
  shop_id,
  count(visit_id) as pageviews
from  
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits 
where 
  platform in ('mobile_web','desktop')
group by all 
)
, listing_counts as (
select
  v.shop_id, 
  count(distinct a.listing_id) as active_listings, 
  pageviews
from  
  visited_shops v
inner join 
  etsy-data-warehouse-prod.rollups.active_listing_basics a
    on cast(a.shop_id as string)=v.shop_id
group by all
)
, shop_gms as (
select
  v.shop_id,
  sum(gms_net) as gms_net,
  count(transaction_id) as transactions
from 
  visited_shops v
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics b
    on v.shop_id=cast(b.shop_id as string)
left join 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans gms
    on b.user_id=gms.seller_user_id
where 
  date >= current_date-365  -- purchases made in last 365 days 
  and active_seller_status=1
group by all 
order by 3 desc
)
select
  case 
    when active_listings < 5 then 'Less than 5'
    when active_listings >= 5 and active_listings  < 10 then '5-9'
    when active_listings  >= 10 and active_listings  < 25 then '10-24'
    when active_listings  >= 25 and active_listings  < 50 then '25-49'
    when active_listings  >= 50 and active_listings  < 75 then '50-74'
    when active_listings  >= 75 and active_listings  < 100 then '75-99'
    when active_listings  >= 100 and active_listings  < 150 then '100-150'
    when active_listings  >= 150 and active_listings  < 200 then '150-199'
    else '200+'
  end as number_of_listings,
	count(distinct l.shop_id) as visited_shops,
  sum(pageviews) as pageviews,
  sum(gms_net) as gms_net,
  sum(transactions) as transactions
from 
  listing_counts l -- visited shops + listings in each shop
left join 
  shop_gms g -- gms / trans for each shop
    on l.shop_id=cast(g.shop_id as string)
group by all 

-- overall counts to confirm
------- gms + trans counts 
-- with agg as (
select
  count(distinct s.shop_id) as shops_w_purchase,
  sum(gms_net) as gms_net,
  count(transaction_id) as transactions,
  count(distinct v.shop_id) as visited_shops_w_purchased,
  sum(case when v.shop_id is not null then gms_net end) as visited_shop_gms_net,
  count(case when v.shop_id is not null then transaction_id end) as visited_shop_transactions,
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans gms
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics s
    on s.user_id=gms.seller_user_id
left join 
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits v 
    on v.shop_id=cast(s.shop_id as string)
where 
  date >= current_date-365 -- transaction in last year 
  and active_seller_status=1 -- is a currently active seller 
group by all 
order by 3 desc

------- visited shops+ active listings 
select
  count(distinct v.shop_id) as shops_visited,
  count(visit_id) as pageviews,
  count(distinct a.listing_id) as total_active_listings,
from  
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits v
inner join 
  etsy-data-warehouse-prod.rollups.active_listing_basics a
    on cast(a.shop_id as string)=v.shop_id
where 
  platform in ('mobile_web','desktop')
group by all 
------------------------------------------------------------------------------------------
-- LISTING VIEWED + ACTIVE LISTINGS BY PRICE 
------------------------------------------------------------------------------------------
select
	case
  	when coalesce((a.price_usd), lv.price_usd) < 1 then 'Less than $1'
    when coalesce((a.price_usd), lv.price_usd) >= 1 and coalesce((a.price_usd), lv.price_usd) < 5 then '$1-$4.99'
    when coalesce((a.price_usd), lv.price_usd) >= 5 and coalesce((a.price_usd), lv.price_usd) < 10 then '$5-$9.99'
    when coalesce((a.price_usd), lv.price_usd) >= 10 and coalesce((a.price_usd), lv.price_usd) < 25 then '$10-$24.99'
    when coalesce((a.price_usd), lv.price_usd) >= 25 and coalesce((a.price_usd), lv.price_usd) < 50 then '$25-$49.99'
    when coalesce((a.price_usd), lv.price_usd) >= 50 and coalesce((a.price_usd), lv.price_usd) < 75 then '$50-$74.99'
    when coalesce((a.price_usd), lv.price_usd) >= 75 and coalesce((a.price_usd), lv.price_usd) < 100 then '$75-$99.99'
    else '100+'
    -- when coalesce((a.price_usd), lv.price_usd) >= 100 and coalesce((a.price_usd), lv.price_usd) < 150 then '$100-$49.99'
    -- when coalesce((a.price_usd), lv.price_usd) >= 50 and coalesce((a.price_usd), lv.price_usd) < 75 then '$50-$4.99'
  end as listing_price,

	count(a.listing_id) as active_listings,

  -- case when a.listing_id is null then 1 else 0 end as missing_in_analytics,
  count(distinct lv.listing_id) as listings_viewed,
	count(lv.visit_id) as listing_views,
  sum(purchased_after_view) as purchases,
  
  count(distinct case when referring_page_event in ('shop_home') then lv.listing_id end) as shop_home_listings_viewed,
	count(case when referring_page_event in ('shop_home') then lv.visit_id end) as shop_home_listing_views,
	sum(case when referring_page_event in ('shop_home') then purchased_after_view end) as shop_home_purchases

from 
  etsy-data-warehouse-prod.rollups.active_listing_basics a
left join 
 (select * from etsy-data-warehouse-prod.analytics.listing_views where _date >=current_date-30) lv 
    on cast(a.listing_id as int64)=lv.listing_id
group by all


------------------------------------------------------------------------------------------
-- LISTING VIEWED + ACTIVE LISTINGS BY PRICE 
------------------------------------------------------------------------------------------
-- TEST 1: make sure gms counts make sense w each seller 
-- with agg as (
select
  shop_id,
  sum(gms_net) as gms_net,
  count(transaction_id) as transactions
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans gms
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics s
    on s.user_id=gms.seller_user_id
where 
  date >= current_date-365 
  and active_seller_status=1
group by all 
order by 3 desc
limit 5
-- shop_id	gms_net	transactions
-- 10967397	7120186.31221499	537011
-- 10204022	12455786.76998739	507335
-- 11779782	1784047.52131355	390411
-- 20230277	4108186.0001604	387690
-- 10162345	1049477.07700062	299642

select distinct user_id from etsy-data-warehouse-prod.rollups.seller_basics where shop_id = 10204022
select sum(gms_net), count(transaction_id) from etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans where seller_user_id= 55496650 and date >= current_date-365 

-- TEST 2: make sure listing counts make sense for each shop
select
  v.shop_id, 
  count(distinct a.listing_id) as active_listings
from  
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits v
inner join 
  etsy-data-warehouse-prod.rollups.active_listing_basics a
    on cast(a.shop_id as string)=v.shop_id
group by all
order by 2 desc
limit 5
-- shop_id	active_listings
-- 24218839	74308
-- 5400716	61194
-- 14280094	50165
-- 16405631	35167
-- 5413707	33789

select shop_name from etsy-data-warehouse-prod.rollups.seller_basics where shop_id = 5400716

select 74197/74308

--TEST 3: check each cte as specific shop level
with visited_shops as (
select
  shop_id,
  count(visit_id) as pageviews
from  
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits 
where 
  platform in ('mobile_web','desktop')
group by all 
)
, listing_counts as (
select
  v.shop_id, 
  count(distinct a.listing_id) as active_listings, 
  pageviews
from  
  visited_shops v
inner join 
  etsy-data-warehouse-prod.rollups.active_listing_basics a
    on cast(a.shop_id as string)=v.shop_id
group by all
)
-- select shop_id, count(*) from listing_counts group by all order by 2 desc limit 5
-- select * from listing_counts where shop_id in ('52155281')

, shop_gms as (
select
  v.shop_id,
  sum(gms_net) as gms_net,
  count(transaction_id) as transactions
from 
  visited_shops v
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics b
    on v.shop_id=cast(b.shop_id as string)
left join 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans gms
    on b.user_id=gms.seller_user_id
where 
  date >= current_date-365  -- purchases made in last 365 days 
  and active_seller_status=1
group by all 
order by 3 desc
)
select
  case 
    when active_listings < 5 then 'Less than 5'
    when active_listings >= 5 and active_listings  < 10 then '5-9'
    when active_listings  >= 10 and active_listings  < 25 then '10-24'
    when active_listings  >= 25 and active_listings  < 50 then '25-49'
    when active_listings  >= 50 and active_listings  < 75 then '50-74'
    when active_listings  >= 75 and active_listings  < 100 then '75-99'
    when active_listings  >= 100 and active_listings  < 150 then '100-150'
    when active_listings  >= 150 and active_listings  < 200 then '150-199'
    else '200+'
  end as number_of_listings,
	count(distinct l.shop_id) as visited_shops,
  sum(pageviews) as pageviews,
  sum(gms_net) as gms_net,
  sum(transactions) as transactions
from 
  listing_counts l -- visited shops + listings in each shop
left join 
  shop_gms g -- gms / trans for each shop
    on l.shop_id=cast(g.shop_id as string)
where l.shop_id in ('52155281')
group by all 

-- shop_id	f0_
-- 52155281	1
------- 5 active listings, 42 pageviews, 8 gms, 2 transactions, ACCURATE IN END ROUNDUP 
-- 10072323	1
-- 10792869	1
-- 13304400	1
-- 39582769	1

SELECT * FROM  etsy-data-warehouse-prod.rollups.seller_basics WHERE SHOP_ID =52155281
