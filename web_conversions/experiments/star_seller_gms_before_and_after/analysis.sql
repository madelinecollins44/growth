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
  and (_date >= ('2025-06-01') and _date <= ('2025-07-17')) -- only looking between 6/1 and 7/17
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
  and _date <= ('2025-07-17')
group by all 
); 

--------------------------------------------------------------------------------
-- ADDING TOGETHER TRANS DATA WITH VISIT DATA
--------------------------------------------------------------------------------
with trans as (
select
  _date, 
  split(visit_id, ".")[0] as browser_id, 
  shop_id,
  sum(trans_gms_net) as trans_gms_net,
  count(distinct transaction_id) as transactions
from 
  etsy-data-warehouse-prod.visit_mart.visits_transactions vt
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics sb 
    on vt.seller_user_id=sb.user_id
where 1=1
  -- and transaction_live=1 -- trans is still live
  and cast(_date as string) >= ('2025-06-01') and cast(_date as string) <= ('2025-07-17') -- only looking between 6/1 and 7/17
group by 1,2,3
)
, traffic as (
select
  _date,
  browser_id, 
  ht.shop_id,
  case when ssd.shop_id is not null then 1 else 0 end as star_seller_status,
  sum(visits) as total_visits
from 
  etsy-data-warehouse-dev.madelinecollins.holder_table ht
left join 
    (select
      distinct shop_id 
    from 
      etsy-data-warehouse-prod.star_seller.star_seller_daily 
    where 1=1
      and (_date >= ('2025-06-01') and _date <= ('2025-07-17'))
      and is_star_seller is true) ssd 
  on cast(ssd.shop_id as string)=ht.shop_id
group by 1,2,3,4
)
select
  case 
    when tfc._date >= ('2025-06-01') and tfc._date <= ('2025-06-15') then 'before' -- two weeks before experiment 
    when tfc._date >= ('2025-07-03') and tfc._date <= ('2025-07-17') then 'after' -- two weeks after experiment 
    else 'during'
  end as experiment_period,
  coalesce(star_seller_status,0) as star_seller_status,
  count(distinct tfc.browser_id) as browser_visits,
  sum(total_visits) as total_visits,
  count(distinct trns.browser_id) as browser_converts,
  sum(transactions) as total_transactions,
  sum(trans_gms_net) as total_gms,
from 
  traffic tfc
left join 
  trans trns 
    on tfc.browser_id=trns.browser_id
    and cast(trns.shop_id as string)=tfc.shop_id
    -- and tfc._date=date(timestamp_seconds(_date))
group by 1,2
