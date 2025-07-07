-- Goals sheet: https://docs.google.com/document/d/1RH-0BLqccFmjo7tKqwxUXO3l5BzI6YkSEK2Np2utKiM/edit?tab=t.i8sbskucyn8u#heading=h.lkkwn0bkwqo0


-------------------------------------------------------
--GLOBAL COVERAGE
-------------------------------------------------------
select
  platform,
  count(distinct visit_id) as total_visits,
  sum(total_gms) as gms
from etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30
group by all
  
-------------------------------------------------------
-- PAGE SURFACE TRAFFIC (last 30 days)
-------------------------------------------------------
with surface_traffic as (
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
  count(distinct a.visit_id) as surface_visits,
  count(distinct case when converted > 0 then a.visit_id end ) as converted_surface_visits,
  sum(total_gms) as surface_gms 
from 
  surface_traffic a
inner join 
  etsy-data-warehouse-prod.weblog.visits b using (visit_id)
where 
  b._date >= current_date-30
  -- and b.platform in ('mobile_web','desktop')
group by all
  

-------------------------------------------------------
-- LISTING PAGE COVERAGE OF LISTINGS WITHOUT REVIEWS
-------------------------------------------------------
with reviews as (
select
  listing_id,
  count(distinct transaction_id) as total_reviews,
  -- count(distinct case when date(transaction_date) >= current_date-365 then transaction_id end) reviews_in_last_year,
  -- count(distinct case when date(transaction_date) < current_date-365 then transaction_id end) reviews_in_before_last_year
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews  
where 
  has_review > 0 -- only listings 
group by all
)
, views as (
select
  platform,
  listing_id,
  visit_id,
  count(sequence_number) as total_views,
  sum(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  -- and platform in ('desktop','mobile_web','boe')
group by all
)
, engagements as (
select
  platform,
	visit_id,
  (regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id,
  count(sequence_number) as events 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
inner join 
  etsy-data-warehouse-prod.weblog.visits using (visit_id)
where
	date(_partitiontime) >= current_date-30
  and _date >= current_date-30
	and beacon.event_name in ('listing_page_review_engagement_frontend')
  and platform in ('mobile_web','desktop')
group by all 
)
select
  v.platform,
  count(distinct case when r.total_reviews is null or r.total_reviews= 0 then v.visit_id end) as lv_for_listings_wo_reviews,
  count(distinct case when (r.total_reviews is null or r.total_reviews= 0) and (e.visit_id is not null and e.listing_id is not null) then v.visit_id end) as lv_w_engagements_for_no_reviews,
  count(distinct case when (r.total_reviews is null or r.total_reviews= 0) and (e.visit_id is not null and e.listing_id is not null) and (purchases > 0) then v.visit_id end) as lv_w_engagement_and_purchase_wo_reviews,
from
  views v 
left join 
  engagements e
    on cast(v.listing_id as string)=e.listing_id
    and v.visit_id=e.visit_id
left join 
  reviews r 
    on v.listing_id=r.listing_id 
group by all 


--------------------------------------------------------------------------------------------------------------
-- LISTING PAGE COVERAGE OF LISTINGS WITHOUT RATINGS (no reviews in last 365 days, but has reviews)
--------------------------------------------------------------------------------------------------------------
with reviews as (
select
  listing_id,
  -- count(distinct transaction_id) as total_reviews,
  count(distinct case when date(transaction_date) >= current_date-365 then transaction_id end) reviews_in_last_year,
  count(distinct case when date(transaction_date) < current_date-365 then transaction_id end) reviews_before_last_year
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews  
where 
  has_review > 0 -- only listings 
group by all
)
, views as (
select
  platform,
  listing_id,
  visit_id,
  count(sequence_number) as total_views,
  sum(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  -- and platform in ('desktop','mobile_web','boe')
group by all
)
, engagements as (
select
  platform,
	visit_id,
  (regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id,
  count(sequence_number) as events 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
inner join 
  etsy-data-warehouse-prod.weblog.visits using (visit_id)
where
	date(_partitiontime) >= current_date-30
  and _date >= current_date-30
	and beacon.event_name in ('listing_page_review_engagement_frontend')
  and platform in ('mobile_web','desktop')
group by all 
)
select
  v.platform,
  count(distinct v.visit_id) as visits_view_w_listing,
  count(distinct case when r.reviews_in_last_year= 0 and reviews_before_last_year > 0 then v.visit_id end) as visits_view_w_listing_no_rating,-- listing has reviews but not in last year 
  count(distinct case when (r.reviews_in_last_year= 0 and reviews_before_last_year > 0) and (e.visit_id is not null and e.listing_id is not null) then v.visit_id end) as visits_engage_w_listing_no_ratings,
  count(distinct case when (r.reviews_in_last_year= 0 and reviews_before_last_year > 0) and (e.visit_id is not null and e.listing_id is not null) and (purchases > 0) then v.visit_id end) as visits_engage_and_purchase_w_listing_no_ratings,
from
  views v 
left join 
  engagements e
    on cast(v.listing_id as string)=e.listing_id
    and v.visit_id=e.visit_id
left join 
  reviews r 
    on v.listing_id=r.listing_id 
group by all 
	
--------------------------------------------------------------------------------------------------------------
-- LISTING PAGE COVERAGE OF OOAK LISTINGS
--------------------------------------------------------------------------------------------------------------
with ooak_listings as (
select
	listing_id
from
  `etsy-data-warehouse-prod.rollups.active_listing_basics` a
where 
  a.quantity = 1 
  and (total_gms = 0 or total_gms is null)
)
, views as (
select
  platform,
  listing_id,
  visit_id,
  count(sequence_number) as total_views,
  sum(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  -- and platform in ('desktop','mobile_web','boe')
group by all
)
, engagements as (
select
  platform,
	visit_id,
  (regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id,
  count(sequence_number) as events 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
inner join 
  etsy-data-warehouse-prod.weblog.visits using (visit_id)
where
	date(_partitiontime) >= current_date-30
  and _date >= current_date-30
	and beacon.event_name in ('listing_page_review_engagement_frontend')
  and platform in ('mobile_web','desktop')
group by all 
)
select
  v.platform,
  count(distinct v.visit_id) as visit_w_lv,
  count(distinct case when o.listing_id is not null then v.visit_id end) as visits_view_ooak_listings, -- if listing hasnt been purchased or has no listings
  count(distinct case when o.listing_id is not null and (e.visit_id is not null and e.listing_id is not null) then v.visit_id end) as visits_engage_w_ooak_listings,
  count(distinct case when o.listing_id is not null and (e.visit_id is not null and e.listing_id is not null) and (purchases > 0) then v.visit_id end) as visits_engage_and_purchase_ooak_listings,
from
  views v 
left join 
  engagements e
    on cast(v.listing_id as string)=e.listing_id
    and v.visit_id=e.visit_id
left join 
  ooak_listings o 
    on v.listing_id=o.listing_id 
group by all 


--------------------------------------------------------------------------------------------------------------
-- LISTING PAGE COVERAGE 
--------------------------------------------------------------------------------------------------------------
with views as (
select
  platform,
  listing_id,
  visit_id,
  count(sequence_number) as total_views,
  sum(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  -- and platform in ('desktop','mobile_web','boe')
group by all
)
, engagements as (
select
  platform,
	visit_id,
  (regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id,
  count(sequence_number) as events 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
inner join 
  etsy-data-warehouse-prod.weblog.visits using (visit_id)
where
	date(_partitiontime) >= current_date-30
  and _date >= current_date-30
	and beacon.event_name in ('listing_page_review_engagement_frontend')
  and platform in ('mobile_web','desktop')
group by all 
)
select
  v.platform,
  count(distinct v.visit_id) as visit_w_lv,
  count(distinct case when (e.visit_id is not null and e.listing_id is not null) then v.visit_id end) as visits_engage_w_listing,
  count(distinct case when (e.visit_id is not null and e.listing_id is not null) and (purchases > 0) then v.visit_id end) as visits_engage_and_purchase_w_listing,
from
  views v 
left join 
  engagements e
    on cast(v.listing_id as string)=e.listing_id
    and v.visit_id=e.visit_id
group by all 

--------------------------------------------------------------------------------------------------------------
-- LISTING PAGE TO SHOP HOME TRAFFIC
--------------------------------------------------------------------------------------------------------------
with shop_home_visits as (
select
  visit_id,
  sequence_number,
  event_type,
  lead(event_type) over (partition by visit_id order by sequence_number) as next_page
from 
  etsy-data-warehouse-prod.weblog.events
where
  _date >= current_date-30
  and page_view=1 
)
select
  platform,
  count(distinct visit_id) as visits_w_lv,
  count(distinct case when next_page in ('shop_home') then visit_id end) as visits_from_lv_to_sh,
  count(distinct case when converted > 0 then visit_id end) as converted_visits,
  count(distinct case when next_page in ('shop_home') and converted > 0 then visit_id end) as converted_visits_from_lv_to_sh,
from 
  shop_home_visits shv
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  event_type in ('view_listing')
  and v.converted > 0 
  and v._date >= current_date-30
group by all 
order by 2 desc
