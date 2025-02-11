-- TASK: 
---- We are running research on two shop home concepts week of 2/10, and we will be using insights from those sessions, engineering effort estimation and data to help us prioritize. 
---- During our sessions we've been taking notes on what data we need, and have summarized them (https://docs.google.com/document/d/1nzjrsqTEU-u5xMvyM-9r88V_fDenc7Kx7S30A3xnRUw/edit?tab=t.0#heading=h.cf3el1ml4gfd) 

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- QUESTION 1: What % of users on web view multiple listings from the same shop from Shop home? 
---- Use case - As a buyer viewing multiple listings from the same shop, I am able to easily access my most recently viewed listings from that shop. 
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
with non_seller_visits as ( -- only look at visits from non- sellers
select
  v.platform,
  v.visit_id,
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile mu
    using (user_id)
where
  mu.is_seller = 0 
  and _date >= current_date-30
)
, shop_home_listing_views as ( -- start with pulling all data on listing views from shop_home page
select
  nsv.platform,
  nsv.visit_id,
  seller_user_id, -- used to distinguish between each seller's shop home
  count(distinct listing_id) as unique_listings_viewed,
  count(visit_id) as listing_views,
  sum(added_to_cart) as added_to_cart,
  sum(favorited) as favorited,
  sum(purchased_after_view) as purchased_after_view,
from 
  non_seller_visits nsv
inner join 
  etsy-data-warehouse-prod.analytics.listing_views lv
    using (visit_id)
where 
  _date = current_date-30
  and lv.platform in ('desktop','mobile_web','boe')
  and referring_page_event in ('shop_home') -- only looking at active shop home pages 
group by all 
)
select  
  platform,
  count(distinct visit_id) as visits_view_listings_from_shop_home,
  sum(listing_views) as shop_home_listing_views,
  count(distinct case when unique_listings_viewed = 1 then visit_id end) as visits_view_1_listing,
  sum(case when unique_listings_viewed = 1 then listing_views end) as sh_listing_views_from_1_listing_per_seller,
  count(distinct case when unique_listings_viewed > 1 then visit_id end) as visits_view_1_plus_listings,
  sum(case when unique_listings_viewed > 1  then listing_views end) as sh_listing_views_from_1_plus_listing_per_seller,
from 
  shop_home_listing_views
group by all 




---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- QUESTION 2: What % of users on web repeat purchase from the same shop? 
---- Use case - As a buyer who has bought something from a shop, I am able to easily reorder it or view similar listings from that same shop. 
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- QUESTION 3:What do CR & GMS coverage & visits look like for shops with sections vs shops without? 
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- QUESTION 4: What % of shops create listings for 'free shipping'?
---- We are considering adding modules that automate merchandising - like New or Best sellers etc. Some sellers create listings for ‘free shipping’
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- QUESTION 5: If we were to add price filters to Shop home what ranges should we consider - how are we best getting to this data?
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
