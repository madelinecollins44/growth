----------------------------------------------------------------
-- etsy-data-warehouse-prod.rollups.seller_basics
----------------------------------------------------------------
select count(distinct shop_id) from etsy-data-warehouse-prod.rollups.seller_basics where active_seller_status = 1
--5813548 unique shops


---------------------------------------------------------------- 
--etsy-data-warehouse-prod.etsy_shard.shop_data 
----------------------------------------------------------------
--unique on shop_id level 
select branding_option, count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_data where status in ('active') group by all 
--14943192 active shops 
-- branding option breakdowns
-- branding_option	f0_
-- 2	2792657
-- 0	11455855
-- 1	85532
-- 4	25289
-- 3	34892
-- 5	548967

----------------------------------------------------------------
-- etsy-data-warehouse-prod.etsy_shard.shop_settings 
----------------------------------------------------------------
select count(shop_id), count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_settings 

----------------------------------------------------------------
-- etsy-data-warehouse-prod.etsy_shard.shop_frequently_asked_questions 
----------------------------------------------------------------
select count(shop_id), count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_settings 

----------------------------------------------------------------
-- etsy-data-warehouse-prod.etsy_shard.shop_about 
----------------------------------------------------------------
select count(shop_id), count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_settings 

----------------------------------------------------------------
-- etsy-data-warehouse-prod.etsy_shard.shop_share_items 
----------------------------------------------------------------
select count(shop_id), count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_settings 

----------------------------------------------------------------
-- etsy-data-warehouse-prod.etsy_shard.shop_seller_personal_details
----------------------------------------------------------------
select count(shop_id), count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_settings 

----------------------------------------------------------------
-- etsy-data-warehouse-prod.etsy_shard.shop_settings 
----------------------------------------------------------------
select count(shop_id), count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_settings 

----------------------------------------------------------------
-- etsy-data-warehouse-prod.etsy_shard.seller_marketing_promoted_offer 
----------------------------------------------------------------
select count(shop_id), count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_settings 
