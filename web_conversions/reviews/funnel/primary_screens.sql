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
      -- 'hp_review_nudger_delivered', --Homepage review nudger is delivered
      'review_form_open', -- Screen 1: Review form opened/loaded
      'multistage_review_form_rating_submit'-- Screen 2: Submit star rating 
      'multistage_review_form_text_submit'-- Screen 3: Submit text review 
      )
group by all 
