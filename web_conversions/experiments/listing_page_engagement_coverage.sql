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

  count(case when event_type in ('listing_page_reviews_content_toggle_opened') then sequence_number end) as toggle_events,
  count(distinct case when event_type in ('listing_page_reviews_content_toggle_opened') then visit_id end) as toggle_visits,
  count(distinct case when event_type in ('listing_page_reviews_content_toggle_opened') then browser_id end) as toggle_browsers,

  count(case when event_type in ('reviews_categorical_tag_clicked') then sequence_number end) as cattag_events,
  count(distinct case when event_type in ('reviews_categorical_tag_clicked') then visit_id end) as cattag_visits,
  count(distinct case when event_type in ('reviews_categorical_tag_clicked') then browser_id end) as cattag_browsers,
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 
  v._date >= current_date-5 -- change this depending on experiment dates 
  and platform in ('mobile_web','desktop')
  and event_type in ('listing_page_review_engagement_frontend','listing_page_reviews_pagination','appreciation_photo_overlay_opened','sort_reviews','reviews_categorical_tag_clicked','listing_page_reviews_content_toggle_opened','reviews_categorical_tag_clicked')
group by all 



-- BEACONS TABLE: need this to pull out sort_reviews property
select
  platform,
  count(case when beacon.event_name  in ('listing_page_review_engagement_frontend') then sequence_number end) as engagement_events,
  count(distinct case when beacon.event_name  in ('listing_page_review_engagement_frontend') then visit_id end) as engagement_visits,
  count(distinct case when beacon.event_name  in ('listing_page_review_engagement_frontend') then browser_id end) as engagement_browsers,
  
  count(case when beacon.event_name  in ('listing_page_reviews_pagination') then sequence_number end) as pagination_events,
  count(distinct case when beacon.event_name  in ('listing_page_reviews_pagination') then visit_id end) as pagination_visits,
  count(distinct case when beacon.event_name  in ('listing_page_reviews_pagination') then browser_id end) as pagination_browsers,

  count(case when beacon.event_name  in ('appreciation_photo_overlay_opened') then sequence_number end) as photo_events,
  count(distinct case when beacon.event_name  in ('appreciation_photo_overlay_opened') then visit_id end) as photo_visits,
  count(distinct case when beacon.event_name  in ('appreciation_photo_overlay_opened') then browser_id end) as photo_browsers,

  count(case when beacon.event_name  in ('sort_reviews') then sequence_number end) as sort_events,
  count(distinct case when beacon.event_name  in ('sort_reviews') then visit_id end) as sort_visits,
  count(distinct case when beacon.event_name  in ('sort_reviews') then browser_id end) as sort_browsers,

  count(case when beacon.event_name  in ('listing_page_reviews_content_toggle_opened') then sequence_number end) as toggle_events,
  count(distinct case when beacon.event_name  in ('listing_page_reviews_content_toggle_opened') then visit_id end) as toggle_visits,
  count(distinct case when beacon.event_name  in ('listing_page_reviews_content_toggle_opened') then browser_id end) as toggle_browsers,

  count(case when beacon.event_name  in ('reviews_categorical_tag_clicked') then sequence_number end) as cattag_events,
  count(distinct case when beacon.event_name  in ('reviews_categorical_tag_clicked') then visit_id end) as cattag_visits,
  count(distinct case when beacon.event_name  in ('reviews_categorical_tag_clicked') then browser_id end) as cattag_browsers,
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  `etsy-visit-pipe-prod`.canonical.visit_id_beacons e using (visit_id)
where 
  v._date >= current_date-5
  and date(_partitiontime) >= current_date-5
  and platform in ('mobile_web','desktop')
  and (
      (beacon.event_name in ('listing_page_review_engagement_frontend','listing_page_reviews_pagination','appreciation_photo_overlay_opened','listing_page_reviews_content_toggle_opened','reviews_categorical_tag_clicked'))
      or
      (beacon.event_name in ('sort_reviews') and (select value from unnest(beacon.properties.key_value) where key = "primary_event_source") in ('view_listing'))
  )
group by all 
