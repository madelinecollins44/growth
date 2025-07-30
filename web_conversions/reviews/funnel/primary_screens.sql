select 
  event_type,
  count(distinct visit_id) as visits,
  count(sequence_number) as views
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e
    using (visit_id)
where 1=1
  and v._date >= current_date-30
  and e._date >= current_date-30
  and platform in ('desktop','mobile_web')
  and event_type in (
      'hp_review_nudger_delivered', -- Getting into reviews: homepage review nudger is delivered
      'review_purchases_nav_v3_click', -- Getting into reviews: Click on header nav "Reviews" link
      'review_form_open', -- Primary Screen 1: Review form opened/loaded
      'choose_your_own_review_card_clicked', -- Primary Screen 1: Select item to review
      'review_attribute_clicked_on_review_form', -- Screen 1: Review form opened/loaded
      'review_form_selected_rating', -- Screen 1: Select star rating

      'multistage_review_form_rating_submit',-- Screen 2: Submit star rating 

      'multistage_review_form_text_submit',-- Screen 3: Submit text review 

      'multistage_review_form_photo_skipped', -- Screen 4: Skip review photo
      'multistage_review_form_photo_submit',-- Screen 4: Submit review photo
      
      'choose_your_own_review_submitted' -- Screen 5: Screen after review was fired
      )
group by all 
