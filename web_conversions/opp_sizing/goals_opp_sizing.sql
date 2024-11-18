-------------------------------------------------------
--GLOBAL COVERAGE
-------------------------------------------------------
select
  count(distinct visit_id) as total_visits,
  sum(total_gms) as gms
from etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30
group by all

-------------------------------------------------------
--LISTING LANDINGS
-------------------------------------------------------

-------------------------------------------------------
--SHOP HOME VISITS 
-------------------------------------------------------
