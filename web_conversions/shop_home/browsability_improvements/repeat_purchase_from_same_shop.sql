create or replace table `etsy-data-warehouse-dev.madelinecollins.last_year_purchase_data` as 
select
  a.date 
  , a.creation_tsz
  , a.buyer_user_id
  , a.mapped_user_id
  , vv.browser_id 
  , vv.visit_id as purchase_visit
  , a.is_guest 
  , a.listing_id 
  , a.seller_user_id
  , sb.shop_id
  , a.receipt_id 
  , a.transaction_id
    -- , a.quantity
    -- , a.usd_price 
    -- , a.trans_shipping_price
    -- , a.usd_subtotal_price
    -- , vv.total_gms as visit_gms
  , case when vv.platform = 'boe' then vv.event_source else vv.platform end as purchased_platform
  -- , vv.detected_region
from `etsy-data-warehouse-prod.transaction_mart.all_transactions` a 
join `etsy-data-warehouse-prod.transaction_mart.transactions_visits` v on a.transaction_id = v.transaction_id 
join `etsy-data-warehouse-prod.weblog.visits` vv on vv.visit_id = v.visit_id  
join etsy-data-warehouse-prod.rollups.seller_basics sb on sb.user_id=a.seller_user_id
where 1=1
  and vv._date >= current_date-365
  and vv.platform in ('boe','desktop','mobile_web')
  and a.date >= current_date-365
  and v.date >= current_date-365
group by all 
;

create or replace table `etsy-data-warehouse-dev.madelinecollins.shop_repurchases` as
select
  mapped_user_id 
  , seller_user_id 
  , count(distinct date) as purchase_days 
  , count(distinct transaction_id) as transactions  
  , count(distinct receipt_id) as receipts  
    --, sum(visit_gms) as total_gms
from `etsy-data-warehouse-dev.madelinecollins.last_year_purchase_data` 
where purchased_platform in ('desktop','mobile_web')
group by 1,2 
;

-- users with multiple shop purchases
  select mapped_user_id, max(purchase_days) as purchase_days
  from `etsy-data-warehouse-dev.madelinecollins.shop_repurchases`
  group by 1 

  -- users and shop with multiple purchases
    select mapped_user_id, seller_user_id, purchase_days
  from `etsy-data-warehouse-dev.csamuelson.shop_repurchases` 
  where 1=1
  and purchase_days > 1 

-- users + # of days purchasing from vairous shops
select  
    count(distinct mapped_user_id) as users,
    count(distinct case when purchase_days = 1 then mapped_user_id end) as one_time,
    count(distinct case when purchase_days > 1 then mapped_user_id end) as more_than_one_time,
    count(distinct case when purchase_days >= 2 then mapped_user_id end) as at_least_two_times,
    count(distinct case when purchase_days >= 3 then mapped_user_id end) as at_least_three_times,
    count(distinct case when purchase_days >= 4 then mapped_user_id end) as at_least_four_times,
    count(distinct case when purchase_days >= 5 then mapped_user_id end) as at_least_five_times,
    count(distinct case when purchase_days >= 10 then mapped_user_id end) as at_least_ten_times,
  from `etsy-data-warehouse-dev.madelinecollins.web_shop_repurchases`
  group by all

-- gms coverage
with days_purchased as (
select
  mapped_user_id,
  seller_user_id,
  purchase_days
from
  `etsy-data-warehouse-dev.madelinecollins.shop_repurchases`
)
, gms as (
select
  mapped_user_id,
  seller_user_id,
  sum(gms_net) as gms_net
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans
where 
  date >= current_date-365
group by all 
)
, agg as (
select
  mapped_user_id,
  seller_user_id,
  gms_net,
  purchase_days
from days_purchased
left join gms using (mapped_user_id, seller_user_id)
)
select
  case 
    when purchase_days = 1 then 'one_time'
    when purchase_days > 1 then 'more_than_one_time'
    when purchase_days >= 2 then 'at_least_two_times'
    when purchase_days >= 3 then 'at_least_three_times'
    when purchase_days >= 4 then 'at_least_four_times'
    when purchase_days >= 5 then 'at_least_five_times'
    when purchase_days >= 10 then 'at_least_ten_times'
  end as purchase_days,
  count(distinct mapped_user_id) as users,
  sum(gms_net) as gms_net
from agg
group by all
    
------------------------
-- TESTING
------------------------
select shop_id, purchase_days from `etsy-data-warehouse-dev.madelinecollins.web_shop_repurchases`  where mapped_user_id = 154488413 
-----------most purchase days 
-- mapped_user_id	shop_id	purchase_days	transactions	receipts
-- 120098149	34938599	362	378	378
-- 244545737	41885050	358	682	682
-- 60259574	38800378	346	1635	1479

-----------3 purchase days 
-- mapped_user_id	shop_id	purchase_days	transactions	receipts
-- 154488413	5051189	3	3	3
-- 49353	5155588	3	3	3

select purchase_days, count(shop_id) from `etsy-data-warehouse-dev.madelinecollins.web_shop_repurchases`  where mapped_user_id = 154488413 group by all 
--single mapped user id (only looked at instances where purchased more than once)
--   shop_id	purchase_days
-- 5272828	4
-- 5051189	3
-- 8262460	3
-- 5671961	3
-- 5479952	3
-- 7032763	3
-- 36519263	2
-- 12311340	2
-- 6458109	2
-- 5202751	2
-- 23135976	1
---------purchase days by @# of shops
-- purchase_days	f0_
-- 1	28
-- 2	4
-- 3	5
-- 4	1
  select  
    count(distinct mapped_user_id) as users,
    count(distinct case when purchase_days = 1 then mapped_user_id end) as one_time,
    count(distinct case when purchase_days > 1 then mapped_user_id end) as more_than_one_time,
    count(distinct case when purchase_days >= 2 then mapped_user_id end) as at_least_two_times,
    count(distinct case when purchase_days >= 3 then mapped_user_id end) as at_least_three_times,
    count(distinct case when purchase_days >= 4 then mapped_user_id end) as at_least_four_times,
    count(distinct case when purchase_days >= 5 then mapped_user_id end) as at_least_five_times,
    count(distinct case when purchase_days >= 10 then mapped_user_id end) as at_least_ten_times,
  from `etsy-data-warehouse-dev.madelinecollins.web_shop_repurchases`
  where mapped_user_id = 154488413
  group by all

--ALL GMS FROM LAST 365 DAYS
select
  sum(gms_net) as gms_net
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans
where 
  date >= current_date-365
group by all 
--10781194552.3956664
