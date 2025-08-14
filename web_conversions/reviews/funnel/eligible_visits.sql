with all_receipts as (
select
  receipt_id,
  initial_edd_min,
  date_diff(initial_edd_min, current_date,day) as dates_between_edd
from etsy-data-warehouse-prod.rollups.receipt_shipping_basics
group by 1,2,3
)
, eligible_receipts as (
select 
  distinct receipt_id 
from all_receipts
where dates_between_edd <= 100 -- can leave reviews up to 100 days after edd
)
, browsers_for_receipts as (
select
  split(visit_id, ".")[0] as browser_id, 
  count(distinct receipt_id) -- elgibile receipts for reviews
from 
  etsy-data-warehouse-prod.transaction_mart.receipts_visits
inner join  
  eligible_receipts
    using (receipt_id)
group by 1
)
-- , browser_visits as (
select 
  count(distinct v.visit_id) as total_visits,
  count(distinct case when event_type in ('hp_review_nudger_delivered') then v.visit_id end) as visits_w_nudge,
  count(distinct case when event_type in ('review_purchases_nav_v3_click') then v.visit_id end) as visits_w_nav_clicks,
  count(distinct case when event_type in ('choose_your_own_review_card_clicked', 'review_form_open') then v.visit_id end) as visits_w_review_starts,
  count(distinct v.browser_id) as total_browsers,
  count(distinct case when event_type in ('hp_review_nudger_delivered') then v.browser_id end) as browsers_w_nudge,
  count(distinct case when event_type in ('review_purchases_nav_v3_click') then v.browser_id end) as browsers_w_nav_clicks,
  count(distinct case when event_type in ('choose_your_own_review_card_clicked', 'review_form_open') then v.browser_id end) as browsers_w_review_starts,
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e
    using (visit_id)
inner join 
  browsers_for_receipts b
    on v.browser_id=b.browser_id 
where 1=1
  and v._date >= current_date-30
  and e._date >= current_date-30
  and platform in ('desktop','mobile_web')
-- )
