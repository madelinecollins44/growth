select 
  _date,
  coverage_value,
from 
  etsy-data-warehouse-prod.catapult.results_coverage_day
where launch_id =1371917948360
and coverage_name in ('GMS coverage')
and unit in ('PERCENTAGE')
and lower(segmentation) in ('any')
and lower(segment) in('all')
order by 1 asc
