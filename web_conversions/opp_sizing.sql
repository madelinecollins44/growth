------------------------------------------------------------
--BUYER SEGMENT
------------------------------------------------------------

------------------------------------------------------------
--REPORTING CHANNEL
------------------------------------------------------------

------------------------------------------------------------
--LISTING CATEGORY
------------------------------------------------------------

------------------------------------------------------------
--PLATFORM
------------------------------------------------------------
select
  browser_platform,
  platform,
  count(distinct visit_id) as traffic,
  count(distinct case when converted > 0 then visit_id end) as converted_visits,
  sum(total_gms) as total_gms
from 
  etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30
group by all 

------------------------------------------------------------
--ENTRY VS EXIT POINTS
------------------------------------------------------------
--landing events
--referrers to listing page
