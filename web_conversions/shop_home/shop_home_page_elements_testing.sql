----------------------------------------------------------------
-- etsy-data-warehouse-prod.rollups.seller_basics
----------------------------------------------------------------
select count(distinct shop_id) from etsy-data-warehouse-prod.rollups.seller_basics where active_seller_status = 1
--5813548 unique shops

---------------------------------------------------------------- 
--etsy-data-warehouse-prod.etsy_shard.shop_data 
----------------------------------------------------------------
--unique on shop_id level 
select count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_data where status in ('active') group by all 
--14943192 active shops 

select branding_option, count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_data where status in ('active') group by all 
-- branding option breakdowns
-- branding_option	f0_
-- 2	2792657
-- 0	11455855
-- 1	85532
-- 4	25289
-- 3	34892
-- 5	548967

select message, count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_data where status in ('active') group by all order by 2 desc limit 5 

----------------------------------------------------------------
-- etsy-data-warehouse-prod.etsy_shard.shop_settings 
----------------------------------------------------------------
-- not unique on shop_id level 
select count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_settings
-- 50260932 unique shops 

select distinct name from etsy-data-warehouse-prod.etsy_shard.shop_settings

select distinct value from etsy-data-warehouse-prod.etsy_shard.shop_settings where name in ('machine_translation')
-- only off
select distinct value from etsy-data-warehouse-prod.etsy_shard.shop_settings where name in ('custom_orders_opt_in')
--f, t
select distinct value from etsy-data-warehouse-prod.etsy_shard.shop_settings where name in ('hide_shop_home_page_sold_items')
-- f,t

select count(distinct shop_id), count(distinct case when name in ('machine_translation')and value = 'off' then shop_id end), count(distinct case when name in ('machine_translation')and value != 'off' then shop_id end) from etsy-data-warehouse-prod.etsy_shard.shop_settings 
-- 50260932	79824	0

select count(distinct shop_id), count(distinct case when name in ('custom_orders_opt_in') then shop_id end), count(distinct case when name in ('custom_orders_opt_in') and value = 't' then shop_id end), count(distinct case when name in ('custom_orders_opt_in')and value = 'f' then shop_id end) from etsy-data-warehouse-prod.etsy_shard.shop_settings 
-- 50260932	3088760	1751139	1337637

select count(distinct shop_id), count(distinct case when name in ('hide_shop_home_page_sold_items') then shop_id end), count(distinct case when name in ('hide_shop_home_page_sold_items') and value = 't' then shop_id end), count(distinct case when name in ('hide_shop_home_page_sold_items')and value = 'f' then shop_id end) from etsy-data-warehouse-prod.etsy_shard.shop_settings 
-- 50260932	2582904	879869	1703052

----------------------------------------------------------------
-- etsy-data-warehouse-prod.etsy_shard.shop_frequently_asked_questions 
----------------------------------------------------------------
-- not unique on shop_id level
select count(shop_id), count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_frequently_asked_questions 
-- 732302 active shops

select count(distinct shop_id), count(shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_frequently_asked_questions where faq_id is null 
--no cases when faq_id or question is null 

----------------------------------------------------------------
-- etsy-data-warehouse-prod.etsy_shard.shop_about 
----------------------------------------------------------------
-- not unique on shop_id level, but is unique when shops are active 
select count(shop_id), count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_about where status in ('active')
--4496729 shops 

select count(distinct case when story != "" and story_headline != "" then shop_id end), count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_about where status in ('active')
-- 2306259	4496729

select * from etsy-data-warehouse-prod.etsy_shard.shop_about where  story = "" and story_headline = "" limit 5

----------------------------------------------------------------
-- etsy-data-warehouse-prod.etsy_shard.shop_share_items 
----------------------------------------------------------------
select count(shop_id), count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_settings 

----------------------------------------------------------------
-- etsy-data-warehouse-prod.etsy_shard.shop_seller_personal_details
----------------------------------------------------------------
select count(shop_id), count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_settings 

----------------------------------------------------------------
-- etsy-data-warehouse-prod.etsy_shard.shop_sections
----------------------------------------------------------------
--not unique on shop_id level
select count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_sections
-- 7619213 shops with sections 

select count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_sections where name is null  
-- not instances where name is null, so everything in this table is a shop with a section 

----------------------------------------------------------------
-- etsy-data-warehouse-prod.etsy_shard.seller_marketing_promoted_offer 
----------------------------------------------------------------
select count(shop_id), count(distinct shop_id) from etsy-data-warehouse-prod.etsy_shard.shop_settings 
