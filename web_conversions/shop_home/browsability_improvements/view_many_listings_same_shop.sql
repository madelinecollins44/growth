//++ --"What % of users on web view multiple listings from the same shop from Shop home? 
//++ -- Use case - As a buyer viewing multiple listings from the same shop, I am able to easily access my most recently viewed listings from that shop."

---------------------------------------------------------------------------------------------------------------------------------------
-- overall page traffic over last 30 days by platform
---------------------------------------------------------------------------------------------------------------------------------------
-- overall page traffic over last 30 days 
select 
  platform,
  count(distinct visit_id)
from 
  etsy-data-warehouse-prod.weblog.events e
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date-30 
  and v.platform in ('mobile_web','desktop','boe')
  and e.event_type in ('shop_home')
group by all
order by 1 asc

-- listing views from shop home referrer on web over last 30 days
select 
  count(distinct visit_id) 
from etsy-data-warehouse-prod.analytics.listing_views 
where _date >= current_date-30 
    and platform in ('mobile_web','desktop') 
    and referring_page_event in ('shop_home')

---------------------------------------------------------------------------------------------------------------------------------------
-- all visits to view multiple listings from the same shop
---- does not exclude sellers or self-visits
---------------------------------------------------------------------------------------------------------------------------------------
with active_listings as ( -- need this to pull in shop_id
select
  listing_id,
  shop_id
from etsy-data-warehouse-prod.rollups.active_listing_basics
)
, shop_home_listing_views as ( -- start with pulling all data on listing views from shop_home page. this is all at the shop_id level. 
select
  -- lv.platform,
  visit_id,
  shop_id,
  count(distinct listing_id) as unique_listings_viewed,
  count(visit_id) as listing_views,
  sum(purchased_after_view) as purchased_after_view,
from 
  etsy-data-warehouse-prod.analytics.listing_views
inner join  
  active_listings using (listing_id)
where 
  _date >= current_date-30 
  and platform in ('mobile_web','desktop') 
  and referring_page_event in ('shop_home')
group by all 
)
select  
  -- platform,
  case -- unique listings viewed from a single seller
    when unique_listings_viewed = 1 then '1'
    when unique_listings_viewed = 2 then '2'
    when unique_listings_viewed = 3 then '3'
    when unique_listings_viewed = 4 then '4'
    when unique_listings_viewed = 5 then '5'
    when unique_listings_viewed between 6 and 10 then '6-10'
    when unique_listings_viewed between 11 and 20 then '11-20'
    when unique_listings_viewed between 21 and 30 then '21-30'
    when unique_listings_viewed between 31 and 40 then '31-40'
    when unique_listings_viewed between 41 and 50 then '41-50'
    else '50+'
  end as unique_listings_viewed,
  count(distinct visit_id) as visits_view_listings_from_shop_home,
  sum(listing_views) as shop_home_listing_views,
from 
  shop_home_listing_views
group by all 
order by 1 asc

---------------------------------------------------------------------------------------------------------------------------------------
-- all visits to view multiple listings from the same shop EXCLUDING sellers
---------------------------------------------------------------------------------------------------------------------------------------
with active_listings as ( -- need this to pull in shop_id
select
  listing_id,
  shop_id
from etsy-data-warehouse-prod.rollups.active_listing_basics
)
, seller_status as (
select
  platform,
  visit_id,
  case when s.user_id is not null and active_seller_status = 1 then 1 else 0 end as is_active_seller,
from 
  etsy-data-warehouse-prod.weblog.visits v
left join  
  etsy-data-warehouse-prod.rollups.seller_basics s 
    using (user_id)
where 
  platform in ('mobile_web','desktop','boe')
  and _date >= current_date-30
)
, shop_home_listing_views as ( -- start with pulling all data on listing views from shop_home page. this is all at the shop_id level. 
select
  s.platform,
  lv.visit_id,
  is_active_seller,
  shop_id,
  count(distinct listing_id) as unique_listings_viewed,
  count(lv.visit_id) as listing_views,
  sum(purchased_after_view) as purchased_after_view,
from 
  etsy-data-warehouse-prod.analytics.listing_views lv
inner join  
  active_listings a using (listing_id)
inner join 
  seller_status s
    on s.visit_id=lv.visit_id
where 
  _date >= current_date-30 
  and referring_page_event in ('shop_home')
group by all 
)
select  
  -- platform,
  is_active_seller,
  case -- unique listings viewed from a single seller
    when unique_listings_viewed = 1 then '1'
    when unique_listings_viewed = 2 then '2'
    when unique_listings_viewed = 3 then '3'
    when unique_listings_viewed = 4 then '4'
    when unique_listings_viewed = 5 then '5'
    when unique_listings_viewed between 6 and 10 then '6-10'
    when unique_listings_viewed between 11 and 20 then '11-20'
    when unique_listings_viewed between 21 and 30 then '21-30'
    when unique_listings_viewed between 31 and 40 then '31-40'
    when unique_listings_viewed between 41 and 50 then '41-50'
    else '50+'
  end as unique_listings_viewed,
  count(distinct visit_id) as visits_view_listings_from_shop_home,
  sum(listing_views) as shop_home_listing_views,
from 
  shop_home_listing_views
group by all 
order by 1 asc

----------------------------------------
--TESTING
----------------------------------------
-- TEST 1: make sure adding in seller_status doesnt mess up counts
with active_listings as ( -- need this to pull in shop_id
select
  listing_id,
  shop_id
from etsy-data-warehouse-prod.rollups.active_listing_basics
)
, seller_status as (
select
  platform,
  visit_id,
  case when s.user_id is not null and active_seller_status = 1 then 1 else 0 end as is_active_seller,
from 
  etsy-data-warehouse-prod.weblog.visits v
left join  
  etsy-data-warehouse-prod.rollups.seller_basics s 
    using (user_id)
where 
  platform in ('mobile_web','desktop')
  and _date >= current_date-30
)
-- , shop_home_listing_views as ( -- start with pulling all data on listing views from shop_home page. this is all at the shop_id level. 
select
  -- s.platform,
  count(distinct lv.visit_id),
  -- is_active_seller,
  -- shop_id,
  -- count(distinct listing_id) as unique_listings_viewed,
  -- count(lv.visit_id) as listing_views,
  -- sum(purchased_after_view) as purchased_after_view,
from 
  etsy-data-warehouse-prod.analytics.listing_views lv
inner join  
  active_listings a using (listing_id)
inner join 
  seller_status s
    on s.visit_id=lv.visit_id
where 
  _date >= current_date-30 
  and referring_page_event in ('shop_home')
  -- and lv.platform in ('mobile_web','desktop')
group by all 
-- 59359600 w sellers, exclude boe 34885893
-- 59359600, exclude boe 34885893
