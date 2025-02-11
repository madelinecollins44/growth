---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- QUESTION 1: What % of users on web view multiple listings from the same shop from Shop home? 
---- Use case - As a buyer viewing multiple listings from the same shop, I am able to easily access my most recently viewed listings from that shop. 
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------
-- SHOP HOME SPECIFIC LISTING VIEWS BY SELLER_USER_ID
----------------------------------------------------------
 
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
  sum(case when unique_listings_viewed = 1 then purchased_after_view end) as sh_purchases_from_1_listing_per_seller,
  count(distinct case when unique_listings_viewed > 1 then visit_id end) as visits_view_1_plus_listings,
  sum(case when unique_listings_viewed > 1  then listing_views end) as sh_listing_views_from_1_plus_listing_per_seller,
  sum(case when unique_listings_viewed > 1 then purchased_after_view end) as sh_purchases_from_1_plus_listing_per_seller,
from 
  shop_home_listing_views
group by all 


------------------------------------
-- OVERALL METRICS TO COMPARE
------------------------------------  
 
with non_seller_visits as ( -- only look at visits from non- sellers
select
  v.platform,
  v.visit_id,
  v.converted,
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile mu
    using (user_id)
where
  mu.is_seller = 0 
  and _date >= current_date-30
  and v.platform in ('desktop','mobile_web','boe')
)
select
  nsv.platform,
  --overall metrics
  count(distinct nsv.visit_id) as visits_from_non_sellers,
  count(distinct listing_id) as unique_listings_viewed,
  count(distinct lv.visit_id) as visits_lv,
  count(distinct case when converted > 0 then nsv.visit_id end) as visit_conversions,
  count(distinct case when converted > 0 then lv.visit_id end) as visit_lv_conversions,
  count(visit_id) as total_lv,
  sum(purchased_after_view) as total_cr,
  -- shop home metrics
  count(distinct case when referring_page_event in ('shop_home') then listing_id end) as sh_unique_listings_viewed,
  count(distinct case when referring_page_event in ('shop_home') then lv.visit_id end) as sh_visits_lv,
  count(distinct case when purchased_after_view > 0 and referring_page_event in ('shop_home') then lv.visit_id end) as sh_visit_conversions,
  count(case when referring_page_event in ('shop_home') then visit_id end) as sh_total_lv,
  sum(case when referring_page_event in ('shop_home') then purchased_after_view end) as sh_total_cr
from 
  non_seller_visits nsv
left join 
  (select * from etsy-data-warehouse-prod.analytics.listing_views where _date = current_date-30) lv
    using (visit_id)
group by all 
