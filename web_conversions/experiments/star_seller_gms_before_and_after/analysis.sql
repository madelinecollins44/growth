/* We removed the value props from the shop home banner in June and the 'star seller' label, leaving only the star seller icon to differentiate. 
Star sellers typically have a higher CR & GMS than non star sellers, so I am curious if we have seen this shift since this launch. This will impact our next steps.
Catapult: https://atlas.etsycorp.com/catapult/1386410949401
*/

--------------------------------------------------------------------------------
-- GET ALL SHOP HOME TRAFFIC INFO 
--------------------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.holder_table as (
with desktop_browsers as (
select
  distinct browser_id
from 
  etsy-data-warehouse-prod.weblog.visits 
where  
  platform in ('desktop')
  and _date >= ('2025-06-01')
)
select
   _date
  , browser_id
  , (select kv.value from unnest(properties.map) as kv where kv.key = "shop_shop_id") as shop_id
  , count(*) as visits 
from 
  etsy-visit-pipe-prod.canonical.beacon_main_2025_06 -- june data
inner join 
  desktop_browsers using (browser_id)
where 1=1
  and event_name = "shop_home"
  and _date is not null 
  and primary_event is true
group by all 
UNION ALL 
select
   _date
  , browser_id
  , (select kv.value from unnest(properties.map) as kv where kv.key = "shop_shop_id") as shop_id
  , count(*) as visits 
from 
  etsy-visit-pipe-prod.canonical.beacon_main_2025_07 -- july data
inner join 
  desktop_browsers using (browser_id)
where 1=1
  and event_name = "shop_home"
  and _date is not null 
  and primary_event is true
group by all 
); 
