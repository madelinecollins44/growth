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
  sum(gms_net) as gms_net,
  sum(case when platform_app in ('desktop','mobile_web') then gms_net end) as web_gms
from
  etsy-data-warehouse-prod.transaction_mart.transactions_visits v 
inner join
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans g
    on g.transaction_id=v.transaction_id 
where 
  v.date >= current_date-365
group by all 
-- gms_net	web_gms
-- 10781194552.3956664	6076948025.65545256



--spot checking popular mapped user ids to make sure gms makes sense
select count(distinct date), sum(gms_net)
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans
where 
  date >= current_date-365
and mapped_user_id = 47709780
and seller_user_id in (856084033,60674841,66291771,359492998,846290405,122124357,94073601,58349168,321629637,75723795,251223435,340620199)
-- 47709780,	273355046	5 days
-- 47709780, (856084033,60674841,66291771,359492998,846290405,122124357,94073601,58349168,321629637,75723795,251223435,340620199), 60,	972298.46154116

-- 47709780	856084033	17
-- 47709780	60674841	14
-- 47709780	66291771	14
-- 47709780	359492998	13
-- 47709780	846290405	13
-- 47709780	122124357	12
-- 47709780	94073601	11
-- 47709780	58349168	11
-- 47709780	321629637	11
-- 47709780	75723795	11
-- 47709780	251223435	10
-- 47709780	340620199	10

/*this is findings purchase days */
select
  mapped_user_id,
  seller_user_id,
  purchase_days
from
  `etsy-data-warehouse-dev.madelinecollins.shop_repurchases`
where mapped_user_id = 47709780
-- mapped_user_id	seller_user_id	purchase_days
-- 47709780	856084033	17
-- 47709780	60674841	14
-- 47709780	66291771	14
-- 47709780	359492998	13
-- 47709780	846290405	13
-- 47709780	122124357	12
-- 47709780	94073601	11
-- 47709780	58349168	11
-- 47709780	321629637	11
-- 47709780	75723795	11
-- 47709780	251223435	10
-- 47709780	340620199	10
-- 47709780	392353313	9
-- 47709780	192648528	9
-- 47709780	316775703	8
-- 47709780	62955543	8
-- 47709780	101810438	8
-- 47709780	195340142	6
-- 47709780	71953561	6
-- 47709780	67906206	6
-- 47709780	775237693	5
-- 47709780	273355046	5
-- 47709780	155486807	4
-- 47709780	357256746	4
-- 47709780	226094904	4
-- 47709780	849793202	4
-- 47709780	201792160	4
-- 47709780	271282099	4
-- 47709780	185841304	4
-- 47709780	98380905	4
-- 47709780	683348809	3
-- 47709780	914764400	3
-- 47709780	423739685	3
-- 47709780	255219913	3
-- 47709780	149120267	3
-- 47709780	74051916	3
-- 47709780	178851570	3
-- 47709780	236066442	2
-- 47709780	95485916	2
-- 47709780	252502137	2
-- 47709780	282533944	2
-- 47709780	221251989	2
-- 47709780	318871051	2
-- 47709780	36498965	2
-- 47709780	218843486	2
-- 47709780	679634652	2
-- 47709780	996976006	2
-- 47709780	153239686	2
-- 47709780	371431072	2
-- 47709780	66164201	2
-- 47709780	615966388	1
-- 47709780	193025818	1
-- 47709780	254164189	1
-- 47709780	130357692	1
-- 47709780	56338001	1
-- 47709780	783483854	1
-- 47709780	267789598	1
-- 47709780	328250068	1
-- 47709780	889415835	1
-- 47709780	281502114	1
-- 47709780	484996900	1
-- 47709780	434132883	1
-- 47709780	921121637	1
-- 47709780	94493575	1
-- 47709780	741671145	1
-- 47709780	86587566	1
-- 47709780	225104767	1
-- 47709780	820259859	1
-- 47709780	148972605	1
-- 47709780	142246511	1
-- 47709780	423355798	1
-- 47709780	51122162	1
-- 47709780	185633906	1
-- 47709780	378550047	1
-- 47709780	761284038	1
-- 47709780	113435210	1
-- 47709780	113885393	1
-- 47709780	638530036	1
-- 47709780	349044866	1
-- 47709780	424130718	1
-- 47709780	908550701	1
-- 47709780	216730962	1
-- 47709780	100661127	1
-- 47709780	199480245	1
-- 47709780	114514168	1
-- 47709780	695977530	1
-- 47709780	61573674	1
-- 47709780	18513317	1
-- 47709780	8139399	1
-- 47709780	523272845	1
-- 47709780	564519282	1
-- 47709780	16216538	1
-- 47709780	6443134	1
-- 47709780	14501565	1
-- 47709780	201406253	1
-- 47709780	26665134	1
-- 47709780	9620156	1
-- 47709780	310987379	1
-- 47709780	355181397	1
-- 47709780	5016424	1
-- 47709780	157593260	1
-- 47709780	193510570	1
-- 47709780	95524522	1
