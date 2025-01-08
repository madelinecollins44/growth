--total desktop visits in the last 30 days
select 
  count(distinct visit_id) as total_visits,
  count(distinct case when platform in ('desktop') then visit_id end) as desktop_traffic,
  count(distinct case when user_id is not null and platform in ('desktop') then visit_id end) as signedin_desktop_traffic,
  count(distinct case when user_id is null and platform in ('desktop') then visit_id end) as signedout_desktop_traffic,
from 
  etsy-data-warehouse-prod.weblog.visits 
where 
 _date >= current_date-30
 and platform in ('desktop')
 group by all 

  
--desktop listing views
select
  count(distinct visit_id) as visits_w_lv,
  count(visit_id) as listing_views
from 
  etsy-data-warehouse-prod.analytics.listing_views
where
  _date >= current_date-30
  and platform in ('desktop')

-- % of lv with review attributes
with listing_views as (
select
  listing_id,
  count(visit_id) as listing_views
from 
  etsy-data-warehouse-prod.analytics.listing_views
where
  _date >= current_date-30
  and platform in ('desktop')
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
