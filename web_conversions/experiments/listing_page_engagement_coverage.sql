select
  platform,
  count(case when event_type in ('listing_page_review_engagement_frontend') then sequence_number end) as engagement_events,
  count(distinct case when event_type in ('listing_page_review_engagement_frontend') then visit_id end) as engagement_visits,
  count(distinct case when event_type in ('listing_page_review_engagement_frontend') then browser_id end) as engagement_browsers,
  
  count(case when event_type in ('listing_page_reviews_pagination') then sequence_number end) as pagination_events,
  count(distinct case when event_type in ('listing_page_reviews_pagination') then visit_id end) as pagination_visits,
  count(distinct case when event_type in ('listing_page_reviews_pagination') then browser_id end) as pagination_browsers,

  count(case when event_type in ('appreciation_photo_overlay_opened') then sequence_number end) as photo_events,
  count(distinct case when event_type in ('appreciation_photo_overlay_opened') then visit_id end) as photo_visits,
  count(distinct case when event_type in ('appreciation_photo_overlay_opened') then browser_id end) as photo_browsers,

  count(case when event_type in ('sort_reviews') then sequence_number end) as sort_events,
  count(distinct case when event_type in ('sort_reviews') then visit_id end) as sort_visits,
  count(distinct case when event_type in ('sort_reviews') then browser_id end) as sort_browsers,
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 
  v._date >= current_date-5
  and platform in ('mobile_web','desktop')
  and event
group by all 
