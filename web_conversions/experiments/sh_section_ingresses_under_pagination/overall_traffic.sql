select
  top_channel,
  platform,
  count(distinct visit_id) as visits,
  count(distinct browser_id) as browsers
from etsy-data-warehouse-prod.weblog.visits
where 1=1
  and platform in ('mobile_web','desktop')
  and _date >= current_date-30
group by all 
