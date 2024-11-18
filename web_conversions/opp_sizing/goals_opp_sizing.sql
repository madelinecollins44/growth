-------------------------------------------------------
--GLOBAL COVERAGE
-------------------------------------------------------
select
  count(distinct visit_id) as total_visits,
  sum(total_gms) as gms
from etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30
group by all
-- total_visits	gms
-- 1130685375	987510850.32

-------------------------------------------------------
--LISTING LANDINGS
-------------------------------------------------------

-------------------------------------------------------
--SHOP HOME VISITS 
-------------------------------------------------------
