------------------------------------------------------------------------------------------------------------------------------------------------------
-- do we have any data on % of shops that run shipping promotions and/or are eligible for free shipping guarantee?
------------------------------------------------------------------------------------------------------------------------------------------------------

-- shops with a free shipping guarantee
select 
  count(distinct shop_id)
from 
  `etsy-data-warehouse-prod.rollups.seller_basics`
left join 
  
where is_frozen = 0
  and active_seller_status = 1
  and buyer_promise_enabled = 1 -- this is the free shipping guarantee  
limit 10
