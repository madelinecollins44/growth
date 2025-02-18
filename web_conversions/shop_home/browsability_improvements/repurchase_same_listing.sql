create or replace table `etsy-data-warehouse-dev.madelinecollins.last_year_purchase_data_category` as 
select
  a.date 
  , a.creation_tsz
  , a.buyer_user_id
  , a.mapped_user_id
  , vv.browser_id 
  , vv.visit_id as purchase_visit
  , a.is_guest 
  , a.listing_id 
  , c.top_category
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
join etsy-data-warehouse-prod.listing_mart.listing_attributes c on a.listing_id=c.listing_id -- looking for listing category
where 1=1
  and vv._date >= current_date-365
  and vv.platform in ('boe','desktop','mobile_web')
  and a.date >= current_date-365
  and v.date >= current_date-365
group by all 
;
