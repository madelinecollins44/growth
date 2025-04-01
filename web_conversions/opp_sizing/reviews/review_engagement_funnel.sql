/* PURPOSE: this funnel is meant to help us understand if there is a correlation between review engagement and conversion, and if this track of work is worth continuing. */

/* total visits in the last 30 days */
select 
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_traffic,
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_traffic,
from 
  etsy-data-warehouse-prod.weblog.visits 
where 
 _date >= current_date-30
 group by all 
  
/* listing views */
select
  count(distinct visit_id) as visits_w_lv,
  count(visit_id) as listing_views,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_visits_w_lv,
  count(case when platform in ('desktop') then visit_id end) as desktop_listing_views,
  count(distinct case when platform in ('mobile_web') then visit_id end) as mweb_visits_w_lv,
  count(case when platform in ('mobile_web') then visit_id end) as mweb_listing_views,
from 
  etsy-data-warehouse-prod.analytics.listing_views
where
  _date >= current_date-30


/* % of lv with review attributes */
with listing_views as (
select
  platform,
  listing_id,
  count(visit_id) as listing_views
from 
  etsy-data-warehouse-prod.analytics.listing_views
where
  _date >= current_date-30
  and platform in ('desktop','mobile_web','boe')
group by all
)
, reviews as (
select
  listing_id,
  sum(has_review) as has_review,
  sum(has_image) as has_image,
  sum(has_video) as has_video,
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews  
group by all
)
select
  platform,
  sum(lv.listing_views) as total_listing_views,
  count(distinct case when r.has_review > 0 then lv.listing_id end) listings_w_review,
  count(distinct case when r.has_image > 0 then lv.listing_id end) listings_w_image,
  count(distinct case when r.has_video > 0 then lv.listing_id end) listings_w_video,
  sum(case when r.has_review > 0 then listing_views end) has_review_lv,
  sum(case when r.has_image > 0 then listing_views end) has_image_lv,
  sum(case when r.has_video > 0 then listing_views end) has_video_lv,
from
  listing_views lv
left join 
  reviews r using (listing_id)
group by all
