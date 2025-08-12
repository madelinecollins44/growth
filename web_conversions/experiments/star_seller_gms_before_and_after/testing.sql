----------------------------------------------------------------------------------------------------
-- TEST 1: see if august beacons table matches with event data while its still in the table 
----------------------------------------------------------------------------------------------------
-- beacons table 
select
   _date
  , browser_id
  -- , (select kv.value from unnest(properties.map) as kv where kv.key = "shop_shop_id") as shop_id
  , count(*) as visits 
from 
  etsy-visit-pipe-prod.canonical.beacon_main_2025_07 -- june data
where 1=1
  and event_name = "shop_home"
  and _date = '2025-07-12'
group by all 
order by 3 desc limit 5 
/* 
_date	browser_id	visits
2025-07-12	D5a2bouxu09VvD2M1lPLFu8d5zgg	15657
2025-07-12	zoRQxvO8Hp9ie_BQANnFkvPyn7qg	6081
2025-07-12	RGRL26AW4MkEfFpVF1mJ0aXUebad	5978
2025-07-12	0998guxbYVQqKd_bC1GChmM-a5td	3371
2025-07-12	SPr7Rxb7eTm6xnI2T6cFOA55CpIx	3154
*/

-- events table 
select 
  _date,
  split(visit_id, ".")[0] as browser_id,
  count(*)
from 
  etsy-data-warehouse-prod.weblog.events
where 
  _date in ('2025-07-12')
  and split(visit_id, ".")[0] in ('SPr7Rxb7eTm6xnI2T6cFOA55CpIx')
  and event_type in ('shop_home')
group by 1,2
/* 
_date	browser_id	f0_
2025-07-12	D5a2bouxu09VvD2M1lPLFu8d5zgg	15591
2025-07-12	zoRQxvO8Hp9ie_BQANnFkvPyn7qg	6422
2025-07-12	RGRL26AW4MkEfFpVF1mJ0aXUebad	5855
2025-07-12	0998guxbYVQqKd_bC1GChmM-a5td	3371
2025-07-12	SPr7Rxb7eTm6xnI2T6cFOA55CpIx	3154*/

----------------------------------------------------------------------------------------------------
-- TEST 2: what % of traffic does not have a shop_id? 
----------------------------------------------------------------------------------------------------
select 
  case when shop_id is null then 0 else 1 end as null_shop,
  sum(visits) as total_visits,
  count(distinct browser_id) as browsers,
from etsy-data-warehouse-dev.madelinecollins.holder_table
group by 1
/* 
null_shop	total_visits	browsers
1	98955548	11772630
0	129716	19971
--------------- LESS THAN 1%
*/
