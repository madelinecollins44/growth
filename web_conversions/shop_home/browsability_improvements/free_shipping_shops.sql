---------------------------------------------------------------------------------------------------------------------------------------
--We are considering adding modules that automate merchandising - like New or Best sellers etc. 
--Some sellers create listings for ‘free shipping’ 🫠- is there a way we can find out what % are like this?
---------------------------------------------------------------------------------------------------------------------------------------
-- better understand free shipping sections
select 
count(distinct name), 
count(distinct case when name like ('%free%shipping%') then name end) as broad_free_shipping_sections, 
count(distinct case when lower(name) in ('free shipping') then name end) as free_shipping_sections,
count(distinct shop_id) as shops,
count(distinct case when name like ('%free%shipping%') then shop_id end) as shops_w_broad_free_shipping_sections,
count(distinct case when lower(name) in ('free shipping') then shop_id end) as shops_w_free_shipping_sections,
from etsy-data-warehouse-prod.etsy_shard.shop_sections 
group by all 
