-------------------------------------------------------
--GLOBAL COVERAGE
-------------------------------------------------------
select
  count(distinct visit_id) as total_visits,
  sum(total_gms) as gms
from etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30
group by all
-- total_visits	gms
-- 1130685375	987510850.32

select
  count(distinct visit_id) as total_visits,
  sum(total_gms) as gms
from etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-90
group by all
-- total_visits	gms
-- 3286564937	2757370099.95

-------------------------------------------------------
--LISTING PAGE LANDINGS (last 90 days)
-------------------------------------------------------
select
  platform,
  count(distinct visit_id) as listing_landing_visits,
  sum(total_gms) as listing_landing_gms
from 
  etsy-data-warehouse-prod.weblog.visits 
where 
  _date >= current_date-90
  and platform in ('mobile_web','desktop')
  and landing_event in ("view_listing", "image_zoom",'listing_page_recommendations','view_sold_listing','view_unavailable_listing','listing__listing_hub__tapped','appreciation_photo_detail')
group by all

-------------------------------------------------------
--SHOP HOME LANDINGS (last 90 days)
-------------------------------------------------------
select
  platform,
  count(distinct visit_id) as shop_home_landings,
  sum(total_gms) as shop_home_gms
from 
  etsy-data-warehouse-prod.weblog.visits 
where 
  _date >= current_date-90
  and platform in ('mobile_web','desktop')
  and landing_event in ('shop_home')
  --and top_channel like ('social_%')
group by all
  
-------------------------------------------------------
--SHOP HOME VISITS (last 30 days)
-------------------------------------------------------
with shop_home_visits as (
select
  distinct visit_id
from 
  etsy-data-warehouse-prod.weblog.events
where 
  _date >= current_date-30
  and event_type in ('shop_home')
)
select
  platform,
  count(distinct a.visit_id) as shop_home_visits,
  sum(total_gms) as shop_home_gms
from 
  shop_home_visits a
inner join 
  etsy-data-warehouse-prod.weblog.visits b using (visit_id)
where 
  b._date >= current_date-30
  and b.platform in ('mobile_web','desktop')
group by all

-------------------------------------------------------
--LISTING PAGE (last 30 days)
-------------------------------------------------------
with lp_visits as (
select
  distinct visit_id
from 
  etsy-data-warehouse-prod.weblog.events
where 
  _date >= current_date-30
  and event_type in ('view_listing')
)
select
  platform,
  count(distinct a.visit_id) as lp_visits,
  sum(total_gms) as lp_gms
from 
  lp_visits a
inner join 
  etsy-data-warehouse-prod.weblog.visits b using (visit_id)
where 
  b._date >= current_date-30
  and b.platform in ('mobile_web','desktop')
group by all

-------------------------------------------------------
--LISTING PAGE WHERE LISTING HAD A REVIEW (last 30 days)
-------------------------------------------------------
with reviews as (
select
  listing_id,
  sum(has_review) as total_reviews
from etsy-data-warehouse-prod.rollups.transaction_reviews
group by all
)
, lv_with_reviews as (
select
  visit_id,
  count(listing_id) as listings_w_review
from  
  etsy-data-warehouse-prod.analytics.listing_views lv
inner join 
  reviews r using (listing_id)
where 
  lv._date >= current_date-30 -- listing views in last 30 days 
  and r.total_reviews > 0 -- only looks at listings with reviews  
group by all 
)
select
  platform,
  count(distinct visit_id) as visits,
  sum(total_gms) as gms
from 
  etsy-data-warehouse-prod.weblog.visits
inner join 
  lv_with_reviews 
   using (visit_id)
where 
    _date >= current_date-30
    and platform in ('mobile_web','desktop')
    and listings_w_review > 0 

--% of listings with reviews
with reviews as (
select
  listing_id,
  sum(has_review) as total_reviews
from etsy-data-warehouse-prod.rollups.transaction_reviews
group by all
)
select 
  count(distinct listing_id) as total_listings,
  count(distinct case when total_reviews = 0 then listing_id end) as listings_wo_reviews,
  count(distinct case when total_reviews > 0 then listing_id end) as listings_w_reviews
from reviews
-- total_listings	listings_wo_reviews	listings_w_reviews
-- 318310912	178270489	140040423
---- 56% of listings dont have a review, 44% of listings have at least one review

--% of listing views by review status
  with reviews as (
select
  listing_id,
  sum(has_review) as total_reviews
from etsy-data-warehouse-prod.rollups.transaction_reviews
group by all
)
-- , lv_with_reviews as (
select
  case 
    when total_reviews = 0 then 'no_reviews' 
    else 'has_reviews'
  end as review_status,
  count(distinct visit_id) as unique_visits,
  count(visit_id) as views,
  count(listing_id) as lv
from  
  etsy-data-warehouse-prod.analytics.listing_views lv
inner join 
  reviews r using (listing_id)
where 
  lv._date >= current_date-30 -- listing views in last 30 days 
group by all 
-- review_status	unique_visits	views	lv
-- no_reviews	166157241	379754094	379754094
-- has_reviews	486092205	1783556677	1783556677

---------------------------------------------------------------------
--LISTING PAGE WHERE LISTING DID NOT HAVE A REVIEW (last 30 days)
---------------------------------------------------------------------
with reviews as (
select
  listing_id,
  sum(has_review) as total_reviews
from etsy-data-warehouse-prod.rollups.transaction_reviews
group by all
)
, lv_wo_reviews as (
select
  visit_id,
  count(listing_id) as listings_wo_review
from  
  etsy-data-warehouse-prod.analytics.listing_views lv
inner join 
  reviews r using (listing_id)
where 
  lv._date >= current_date-30 -- listing views in last 30 days 
  and r.total_reviews < 1 -- only looks at listings without reviews  
group by all 
)
select
  platform,
  count(distinct visit_id) as visits,
  sum(total_gms) as gms
from 
  etsy-data-warehouse-prod.weblog.visits
inner join 
  lv_wo_reviews 
   using (visit_id)
where 
    _date >= current_date-30
    and platform in ('mobile_web','desktop')
group by all

----TESTING
  with reviews as (
select
  listing_id,
  sum(has_review) as total_reviews
from etsy-data-warehouse-prod.rollups.transaction_reviews
group by all
)
select * from reviews where listing_id = 1675270968
-- listing_id without reviews 
-- 1718881800
-- 1689001150
-- 1771603778
-- 1675270968
-- 1759252800

-- listing_ids w reviews
-- 1142035121
-- 951381169
-- 1316226954
-- 784930715
-- 473800867
, lv_without_reviews as (
select
  visit_id,
  listing_id
  -- count(listing_id) as listings_w_review
from  
  etsy-data-warehouse-prod.analytics.listing_views lv
inner join 
  reviews r using (listing_id)
where 
  lv._date >= current_date-30 -- listing views in last 30 days 
  and r.total_reviews < 1 -- only looks at listings without reviews  
group by all 
)
select distinct listing_id from lv_without_reviews limit 5

--avg transaction stats for listings without any reviews
with reviews as (
select
  listing_id,
  count(distinct transaction_id) as total_transactions,
  sum(has_review) as total_reviews
from etsy-data-warehouse-prod.rollups.transaction_reviews
group by all
)
select
  count(distinct listing_id) as listings,
  sum(total_transactions) as total_transactions,
  avg(total_transactions) as avg_transactions
from
  reviews
where total_reviews < 1
-- listings	total_transactions	avg_transactions
-- 178419690	323511140	1.8132031279731542

--viewed listings without reviews
  with reviews as (
select
  listing_id,
  count(distinct transaction_id) as transactions,
  sum(has_review) as total_reviews
from etsy-data-warehouse-prod.rollups.transaction_reviews
group by all
)
select
  count(distinct listing_id) as listings,
  sum(transactions) as total_transactions,
  avg(transactions) as avg_transactions,
  count(lv.visit_id) as views
from  
  etsy-data-warehouse-prod.analytics.listing_views lv
inner join 
  reviews r using (listing_id)
where 
  lv._date >= current_date-30 -- listing views in last 30 days 
  and r.total_reviews < 1 -- only looks at listings without reviews  
group by all 

-------------------------------------------------------
--LISTING REVIEWS SEEN (last 30 days)
-------------------------------------------------------
with lp_reviews_seen as (
select
  distinct visit_id
from 
  etsy-data-warehouse-prod.weblog.events
where 
  _date >= current_date-30
  and event_type in ('listing_page_reviews_seen')
)
select
  platform,
  count(distinct a.visit_id) as lp_reviews_seen_visits,
  sum(total_gms) as lp_reviews_seen_gms
from 
  lp_reviews_seen a
inner join 
  etsy-data-warehouse-prod.weblog.visits b using (visit_id)
where 
  b._date >= current_date-30
  and b.platform in ('mobile_web','desktop')
group by all


--reviews seen by platform
  with lp_reviews_seen as (
select
  distinct visit_id
from 
  etsy-data-warehouse-prod.weblog.events
where 
  _date >= current_date-30
  and event_type in ('listing_page_reviews_seen')
)
select
  platform,
  count(distinct a.visit_id) as lp_reviews_seen_visits,
  sum(total_gms) as lp_reviews_seen_gms
from 
  lp_reviews_seen a
inner join 
  etsy-data-warehouse-prod.weblog.visits b using (visit_id)
where 
  b._date >= current_date-30
  and b.platform in ('mobile_web','desktop')
group by all
-------listing reviews seen by platform
--  platform	    lp_reviews_seen_visits	    lp_reviews_seen_gms
-- mobile_web	    89826933	                      142524194.16
-- desktop	      50682973	                        188206356.8
-------listings seen by platform
-- platform	    lp_visits	    lp_gms
-- desktop	     138986848	   289372317.29
-- mobile_web	   335078617	   227353498.97
---- mobile_web: 26.8% of lv saw the reviews, 62.7% of lv gams saw the review
---- desktop: 36.4% of lv saw the reviews, 65.0% of lv gms saw the review
  
-------global visits / gms coverage for this calc 
-- total_visits	gms
-- 1140444332	1004663981.54
---- mobile_web: 7.8% of visit coverage, 14.2% of gms coverage
---- desktop: 4.4% of visits coverage, 18.7% of gms 

  
-----------------------------------------------------------------
--USERS THAT ADDED TO CART IN SAME LISTING VIEW (last 30 days)
-----------------------------------------------------------------
with atc_lv as (
select
  visit_id,
  count(listing_id) as listing_views,
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  and added_to_cart > 0
group by all
)
select
  platform,
  count(distinct visit_id) as atc_lv_visits,
  sum(total_gms) as atc_lv_gms
from 
  atc_lv
inner join 
  etsy-data-warehouse-prod.weblog.visits 
    using (visit_id)
where 
  _date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all

--------------------------------------------------------------------
--LISTINGS VIEW COVERAGE OF LISTINGS WITH VARIATIONS (last 30 days)
  ---stole variation code from here: https://github.com/etsy/apeermohamed/blob/main/Commit/Variations/variation_attributes_analysis/listing_variation_attributes_dataset.sql
--------------------------------------------------------------------
with listing_variation_level_attributes as (
  select 
    v.*, 
    i.image_id, 
    i.create_date, 
    i.is_deleted
  from `etsy-data-warehouse-prod.rollups.listing_variations_extended` v
  left join `etsy-data-warehouse-prod.etsy_shard.variation_images` i
    on v.listing_variation_id = i.listing_variation_id
    and i.is_deleted = 0
)
, listings_with_variations as (
  select 
    listing_id,
    count(distinct variation_name) as variation_count, 
  from listing_variation_level_attributes
  group by all
)
, listings_w_variations as (
select
  visit_id,
  count(listing_id) as listings_wo_review
from  
  etsy-data-warehouse-prod.analytics.listing_views lv
inner join 
  listings_with_variations r using (listing_id)
where 
  lv._date >= current_date-30 -- listing views in last 30 days 
  and r.variation_count > 0 -- only looks at listings with variations  
group by all 
)
select
  platform,
  count(distinct visit_id) as visits,
  sum(total_gms) as gms
from 
  etsy-data-warehouse-prod.weblog.visits
inner join 
  listings_w_variations 
   using (visit_id)
where 
    _date >= current_date-30
    and platform in ('mobile_web','desktop')
group by all

---- what % of listing views are for listings w variations? 
with listing_variation_level_attributes as (
  select 
    v.*, 
    i.image_id, 
    i.create_date, 
    i.is_deleted
  from `etsy-data-warehouse-prod.rollups.listing_variations_extended` v
  left join `etsy-data-warehouse-prod.etsy_shard.variation_images` i
    on v.listing_variation_id = i.listing_variation_id
    and i.is_deleted = 0
)
, listings_with_variations as (
  select 
    listing_id,
    count(distinct variation_name) as variation_count, 
  from listing_variation_level_attributes
  group by all
)
select
  count(lv.listing_id) as listings_views,
  count(case when v.listing_id is not null then lv.listing_id end) as listings_w_variation_viewed,
  count(distinct lv.listing_id) as listings_viewed,
  count(distinct v.listing_id) as listings_w_variation_viewed
from  
  etsy-data-warehouse-prod.analytics.listing_views lv
left join 
  listings_with_variations v using (listing_id)
where 
  lv._date >= current_date-30 -- listing views in last 30 days 
group by all 
-- listings_views	listings_w_variation_views	listings_viewed	listings_w_variation_viewed_1
-- 2541086019	1431514897	90613485	32573022
--56.3% of listing views are for listings with a variation, 35.9% of all listings viewed have a variation

---what % of active listings have a variation?
with listing_variation_level_attributes as (
  select 
    v.*, 
    i.image_id, 
    i.create_date, 
    i.is_deleted
  from `etsy-data-warehouse-prod.rollups.listing_variations_extended` v
  left join `etsy-data-warehouse-prod.etsy_shard.variation_images` i
    on v.listing_variation_id = i.listing_variation_id
    and i.is_deleted = 0
)
, listings_with_variations as (
  select 
    listing_id,
    count(distinct variation_name) as variation_count, 
  from listing_variation_level_attributes
  group by all
)
select
  count(distinct lv.listing_id) as active_listings,
  count(distinct v.listing_id) as active_listings_w_variation,
from  
  etsy-data-warehouse-prod.rollups.active_listing_basics lv
left join 
  listings_with_variations v using (listing_id)
group by all 
-- active_listings	active_listings_w_variation
-- 128976555	53210049
-- 41.2% of active listings have a variation
------listings without variation check 
-- listing_id
-- 120077541
-- 122565401
-- 120135306
-- 124023659
-- 122801546
------listings with variation check 
-- listing_id
-- 1130064496
-- 1131452534
-- 1131052195
-- 1131319028
-- 1130977707

--------------------------------------------------------------------
--LISTINGS VIEWS OF HIGH STAKE LISTINGS + HAVE REVIEWS
--------------------------------------------------------------------
with reviews as (
select
  listing_id,
  sum(has_review) as total_reviews
from etsy-data-warehouse-prod.rollups.transaction_reviews
group by all
)
, lv_with_reviews as (
select
  visit_id,
  count(listing_id) as listings_w_review
from  
  etsy-data-warehouse-prod.analytics.listing_views lv
inner join 
  reviews r using (listing_id)
where 
  lv._date >= current_date-30 -- listing views in last 30 days 
  and r.total_reviews > 0 -- only looks at listings with reviews  
  and price_usd > 100 -- looks at only high stakes items 
group by all 
)
select
  platform,
  count(distinct visit_id) as visits,
  sum(total_gms) as gms
from 
  etsy-data-warehouse-prod.weblog.visits
inner join 
  lv_with_reviews 
   using (visit_id)
where 
    _date >= current_date-30
    and platform in ('mobile_web','desktop')
    and listings_w_review > 0 
group by all

--------------------------------------------------------------------
--LISTINGS VIEWS OF HIGH STAKE LISTINGS + DONT HAVE REVIEWS
--------------------------------------------------------------------
with reviews as (
select
  listing_id,
  sum(has_review) as total_reviews
from etsy-data-warehouse-prod.rollups.transaction_reviews
group by all
)
, lv_wo_reviews as (
select
  visit_id,
  count(listing_id) as listings_wo_review
from  
  etsy-data-warehouse-prod.analytics.listing_views lv
inner join 
  reviews r using (listing_id)
where 
  lv._date >= current_date-30 -- listing views in last 30 days 
  and r.total_reviews < 1 -- only looks at listings without reviews  
  and price_usd > 100
group by all 
)
select
  platform,
  count(distinct visit_id) as visits,
  sum(total_gms) as gms
from 
  etsy-data-warehouse-prod.weblog.visits
inner join 
  lv_wo_reviews 
   using (visit_id)
where 
    _date >= current_date-30
    and platform in ('mobile_web','desktop')
group by all

--------------------------------------------------------------------
--CHECKOUT NUDGES
--------------------------------------------------------------------
with checkout_nudges as (
select
  distinct visit_id
from 
  etsy-data-warehouse-prod.weblog.events
where 
  _date >= current_date-30
  and event_type in ('resume_checkout_drawer_successfully_loaded')
)
select
  platform,
  count(distinct a.visit_id) as checkout_nudges_visits,
  sum(total_gms) as checkout_nudges_gms
from 
  checkout_nudges a
inner join 
  etsy-data-warehouse-prod.weblog.visits b using (visit_id)
where 
  b._date >= current_date-30
  and b.platform in ('mobile_web','desktop')
group by all

--------------------------------------------------------------------
--PROCEED TO CHECKOUT IN CART
--------------------------------------------------------------------
with checkout_from_cart as (
select
  distinct visit_id
from 
  etsy-data-warehouse-prod.weblog.events
where 
  _date >= current_date-30
  and event_type like ('proceed_to_checkout_with%')
)
select
  platform,
  count(distinct a.visit_id) as checkout_visits,
  sum(total_gms) as checkout_gms
from 
  checkout_from_cart a
inner join 
  etsy-data-warehouse-prod.weblog.visits b using (visit_id)
where 
  b._date >= current_date-30
  and b.platform in ('mobile_web','desktop')
group by all

