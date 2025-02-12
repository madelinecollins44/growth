select 
shop_name,
count(distinct name) 
from etsy-data-warehouse-prod.etsy_shard.shop_sections
inner join etsy-data-warehouse-prod.rollups.seller_basics using (shop_id)
where
  active_listing_count >0 
  and active_seller_status = 1
group by all
