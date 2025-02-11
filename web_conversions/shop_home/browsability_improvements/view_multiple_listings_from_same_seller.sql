
--------
--------
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
select
  nsv.platform,
  --overall metrics
  count(distinct listing_id) as unique_listings_viewed,
  count(distinct lv.visit_id) as visits_lv,
  count(distinct case when purchased_after_view > 0 then lv.visit_id end) as visit_conversions,
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
inner join 
  etsy-data-warehouse-prod.analytics.listing_views lv
    using (visit_id)
where 
  _date = current_date-30
  and lv.platform in ('desktop','mobile_web','boe')
group by all 
