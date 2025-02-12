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
  , a.receipt_id 
  , a.transaction_id
    , a.quantity
    , a.usd_price 
    , a.trans_shipping_price
    , a.usd_subtotal_price
    , vv.total_gms as visit_gms
  , case when vv.platform = 'boe' then vv.event_source else vv.platform end as purchased_platform
  , vv.detected_region
  , la.full_path
  , max(vl.is_vintage) as is_vintage
from `etsy-data-warehouse-prod.transaction_mart.all_transactions` a 
join `etsy-data-warehouse-prod.transaction_mart.transactions_visits` v on a.transaction_id = v.transaction_id 
join `etsy-data-warehouse-prod.weblog.visits` vv on vv.visit_id = v.visit_id  
left join `etsy-data-warehouse-prod.listing_mart.listing_attributes` la on a.listing_id = la.listing_id
left join `etsy-data-warehouse-prod.rollups.vintage_listings` vl on a.listing_id = vl.listing_id
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
from  `etsy-data-warehouse-madelinecollins.last_year_purchase_data`
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
