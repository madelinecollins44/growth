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
  etsy-visit-pipe-prod.canonical.beacon_main_2025_08 -- june data
where 1=1
  and event_name = "shop_home"
  and _date >= current_date-30 
group by all 
order by 3 desc limit 5 
/* 
_date	browser_id	visits
2025-08-06	RGRL26AW4MkEfFpVF1mJ0aXUebad	26594
2025-08-01	D5a2bouxu09VvD2M1lPLFu8d5zgg	14015
2025-08-11	ALJeZ2Fs5OEthxGMDYh1OwHq5_Hl	13151
2025-08-03	RGRL26AW4MkEfFpVF1mJ0aXUebad	11525
2025-08-07	RGRL26AW4MkEfFpVF1mJ0aXUebad	11153
*/

-- events table 
select 
  _date,
  split(visit_id, ".")[0] as browser_id,
  count(*)
from 
  etsy-data-warehouse-prod.weblog.events
where 
  _date in ('2025-08-06')
  and split(visit_id, ".")[0] in ('RGRL26AW4MkEfFpVF1mJ0aXUebad')
  and event_type in ('shop_home')
group by 1,2
/* 
_date	browser_id	visits
2025-08-06	RGRL26AW4MkEfFpVF1mJ0aXUebad	26594
2025-08-01	D5a2bouxu09VvD2M1lPLFu8d5zgg	14015
2025-08-11	ALJeZ2Fs5OEthxGMDYh1OwHq5_Hl	13151
2025-08-03	RGRL26AW4MkEfFpVF1mJ0aXUebad	11525
2025-08-07	RGRL26AW4MkEfFpVF1mJ0aXUebad	11153
*/
