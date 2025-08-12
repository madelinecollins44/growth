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

----------------------------------------------------------------------------------------------------
-- TEST 3: is each cte unique on the joining dimensions?
----------------------------------------------------------------------------------------------------
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
select browser_id, shop_id, _date, count(*) from trans group by 1,2,3 order by 4 desc limit 5
/* 
browser_id	shop_id	_date	f0_
ig2frTbdie8tBBnfuyfE-7wuU_6t	22430169	2025-07-09	1
D5935812FFEB432BBB2425B1E6D6	14945926	2025-06-19	1
kYL18xfkSCOmGsknh3iOqQ	27408169	2025-06-27	1
etUbRL1dgmEiKZijxJaavciBiV4F	40836279	2025-06-01	1
IMXxlkxviZWtjGUI2ZYTY0lBAkj_	42189052	2025-06-09	1
*/

with traffic as (
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
select browser_id, shop_id, _date, count(*) from traffic group by 1,2,3 order by 4 desc limit 5
/* 
browser_id	shop_id	_date	f0_
0EIt1KHTSN0mIyPha0SO_2LYaJhC	53717830	2025-07-01	1
q-G6lgzac2QO2ZMV5HKxOHidyYf4	50397803	2025-06-09	1
7G20q0CkbjbG34TwlWlZ0VBCd06X	45700442	2025-06-18	1
FKRc46mgUGUPryXqMXIySW_pCh5R	51456985	2025-07-03	1
ZcYFJe0KVw5lf0dyMqmMBQOCfWUp	25605241	2025-06-18	1
*/

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
select tfc.browser_id, tfc.shop_id, tfc._date, count(*) 
from 
  traffic tfc
left join 
  trans trns 
    on tfc.browser_id=trns.browser_id
    and cast(trns.shop_id as string)=tfc.shop_id
    -- and tfc._date=date(timestamp_seconds(_date))
group by 1,2,3 order by 4 desc
