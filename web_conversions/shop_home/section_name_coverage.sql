------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Research as highlighted that some sellers find it difficult to decide how to utilize sections-- theme, price, etc. What kinds of sections are sellers currently using? 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
select 
 s.name,
  count(distinct shop_id) as shops
  count(distinct case when active_listing_count > 0 then s.name end) as sections
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
where
  active_seller_status = 1
group by all
