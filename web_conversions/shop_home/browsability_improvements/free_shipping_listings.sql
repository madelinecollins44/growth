with shop_sections as (
select 
  shop_id,
  count(distinct name) as sections
from 
  etsy-data-warehouse-prod.etsy_shard.shop_sections
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics using (shop_id)
where
  active_listing_count > 0 
  and active_seller_status = 1
group by all
)
, shop_visits,
, shop_gms (
select
  seller_user_id,
  sum(gms_net)
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans
where date >= current_date-365
)
