-- get sellers user_ids + shop_ids
with seller_info as ( --grab shop_ids for visits over the last 30 days for applicable visits 
select
  platform,
  visit_id,
  user_id,
  shop_id,
  shop_name
from 
  etsy-data-warehouse-prod.weblog.visits v
left join 
  (select * from etsy-data-warehouse-prod.rollups.seller_basics where active_seller_status = 1) s -- all active sellers 
    using (user_id)
where v._date >= current_date-30
and platform in ('mobile_web','desktop','boe')
)
select 
  count(distinct visit_id), 
  count(distinct case when shop_id is null then visit_id end) as not_sellers,
  count(distinct case when shop_id is not null then visit_id end) as sellers,
from seller_info 
group by all 



, active_listings as ( -- need this to pull in shop_id for each listing, since seller_user_id is null in analytics.listing_views
select
  listing_id,
  shop_id
from etsy-data-warehouse-prod.rollups.active_listing_basics
)
, -- get the associated shop_id with each visit 
-- get shop_id of the listing viewed
-- exclude any listing views where the listing shop_id matches the shop_id associated with that visit/ user 


, shop_id_of_viewed_listings as (
select
  nsv.platform,
  nsv.visit_id,
  al.shop_id,
  count(distinct lv.listing_id) as unique_listings_viewed,
  count(lv.visit_id) as listing_views,
  sum(lv.purchased_after_view) as purchased_after_view,
from 
  non_seller_visits nsv
inner join 
  etsy-data-warehouse-prod.analytics.listing_views lv
    using (visit_id)
inner join 
  active_listings al
    on lv.listing_id=al.listing_id
where 
  _date = current_date-30
  and lv.platform in ('desktop','mobile_web','boe')
  and referring_page_event in ('shop_home') -- only looking at active shop home pages 
group by all 
)
