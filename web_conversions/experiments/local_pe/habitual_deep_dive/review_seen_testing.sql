----------------------------------------------------------------------------------------
-- CHECKING HABITUAL VS NON HABIUTAL REVIEWS 
----------------------------------------------------------------------------------------
select 
  variant_id, 
  case when lower(buyer_segment) in ('habitual') then 1 else 0 end as habitual_segment,
  count(distinct bucketing_id), 
  count(distinct case when top_reviews_events > 0 then bucketing_id end) as browsers_w_top_seen, 
  count(distinct case when mid_reviews_events > 0 then bucketing_id end) as browsers_w_mid_seen, 
  sum(top_reviews_events) as top_reviews_events, 
  sum(mid_reviews_events) as mid_reviews_events,
  max(top_reviews_events) as max_top_reviews_events, 
  max(mid_reviews_events) as max_mid_reviews_events
from 
  etsy-bigquery-adhoc-prod._scriptf559414066a34ea7573bf7d307f319636261725a.browsers_with_key_event group by all 
