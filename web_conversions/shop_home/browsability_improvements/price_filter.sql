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

-- number of listings in visited shops
with listing_counts as (
select
  v.shop_id, 
  count(distinct a.listing_id) as active_listings
from  
  etsy-data-warehouse-dev.madelinecollins.web_shop_visits v
inner join 
  etsy-data-warehouse-prod.rollups.active_listing_basics a
    on cast(a.shop_id as string)=v.shop_id
group by all
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
	count(distinct shop_id) as shops
from 
  listing_counts 
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
