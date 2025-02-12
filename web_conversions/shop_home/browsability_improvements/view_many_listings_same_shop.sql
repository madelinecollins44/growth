//--"What % of users on web view multiple listings from the same shop from Shop home? 
//-- Use case - As a buyer viewing multiple listings from the same shop, I am able to easily access my most recently viewed listings from that shop."

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
  lv.platform,
  lv.visit_id,
  al.shop_id,
  count(distinct lv.listing_id) as unique_listings_viewed,
  count(lv.visit_id) as listing_views,
  sum(lv.purchased_after_view) as purchased_after_view,
from 
  etsy-data-warehouse-prod.analytics.listing_views lv
inner join 
  active_listings al
    on lv.listing_id=al.listing_id
where 
  _date = current_date-30
  and lv.platform in ('desktop','mobile_web') --,'boe')
  and referring_page_event in ('shop_home') -- only looking at active shop home pages 
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
