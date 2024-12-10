-- find events counts associated with review process by platform
select
  event_type,
  count(case when platform in ('mobile_web') then visit_id end) as mweb_event_count,
  count(case when platform in ('desktop') then visit_id end) as desktop_event_count,
from  
  etsy-data-warehouse-prod.weblog.events e
inner join  
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date-30
  and event_type in (
      'reviewable_single_item_page_load',
      'review_purchases_nav_v3_click',
      'choose_your_own_review_card_clicked', -- main event for screen 1
      'review_attribute_clicked_on_review_form',
      'review_form_selected_rating',
      'multistage_review_form_rating_submit',
      'multistage_review_form_text_submit',
      'multistage_review_form_photo_skipped',
      'multistage_review_form_photo_submit')
group by all
