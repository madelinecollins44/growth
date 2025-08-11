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


-- events table 
