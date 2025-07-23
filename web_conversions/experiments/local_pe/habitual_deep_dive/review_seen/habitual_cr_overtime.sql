select 
  _date,
  -- segmentation,
  -- segment,
  metric_display_name,
  metric_value_control,
  metric_value_treatment,
  (relative_change/ 100) as relative_change,
  is_significant,
from 
  etsy-data-warehouse-prod.catapult.results_metric_day
where 1=1
  and launch_id = 1371917948360
  and segmentation in ('buyer_segment')
  and lower(segment) in ('habitual')
  and lower(metric_display_name) in ('conversion rate')
order by 1 asc 
