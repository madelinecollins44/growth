with experiments as (
select
  launch_id,
  max(_date) as most_recent_date
from 
  etsy-data-warehouse-prod.catapult.results_metric_day 
)
, coverages as (
select 
  launch_id,
  coverage_name,
  coverage_value,
from 
  etsy-data-warehouse-prod.catapult.results_coverage_day rcd
inner join 
  experiments e
   on rcd.launch_id=e.launch_id
   and rcd._date=e.most_recent_date
where 1=1
  and coverage_name in ('GMS coverage','Traffic coverage')
  and unit in ('PERCENTAGE')
  and lower(segmentation) in ('any')
  and lower(segment) in('all')
)
select
  metric_display_name,
  metric_value_control,
  metric_value_treatment,
  relative_change,
  p_value
from 
  etsy-data-warehouse-prod.catapult.results_metric_day rmd
inner join 
  experiments e
   on rms.launch_id=e.launch_id
   and rmd._date=e.most_recent_date
where 1=1
  and segmentation in ('any')
  and segment in ('all')
  and metric_id in (
      '1029227163677', -- CR
      '1275588643427', -- GMS per Unit
      '1227229423992', -- Ads CR
  )

