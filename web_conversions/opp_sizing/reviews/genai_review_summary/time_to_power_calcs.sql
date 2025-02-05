with active_english_listings as (
select
  alb.listing_id,
  top_category
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics alb
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics sb using (shop_id)
where 
  active_seller_status=1 -- active sellers 
  and primary_language in ('en-US') -- only shops with english/ us as primary language 
  and sb.country_name in ('United States') -- only US sellers 
)
-- text reviews that are in english
, reviews as (
select
  listing_id,
  count(transaction_id) as review_count,
from  
  active_english_listings
inner join 
  etsy-data-warehouse-prod.rollups.transaction_reviews using (listing_id)
where 
  has_text_review > 0  
  and language in ('en')
group by all
having count(transaction_id) >= 5 and count(transaction_id) <= 300
order by 2 desc
)
, listing_views as (
select
  v.platform,
	listing_id,
  a.visit_id,
  case when converted > 0 then 1 else 0 end as converted_visit,
	count(a.visit_id) as listing_views,	
  sum(purchased_after_view) as total_purchases,
  case when purchased_after_view > 0 then 1 else 0 end as converted_on_that_listing,
from 
  etsy-data-warehouse-prod.analytics.listing_views a 
inner join 
  etsy-data-warehouse-prod.weblog.visits v 
    on a.visit_id=v.visit_id
where 
  v._date >=current_date-30
  and a._date >=current_date-30
  and v.platform in ('mobile_web','desktop')
group by all
)
select
  platform,
  count(distinct lv.listing_id) as unique_listings,
  sum(listing_views) as listing_views,
  count(distinct lv.visit_id) as unique_visits,
  sum(total_purchases) as total_purchases,
  count(distinct case when lv.converted_visit > 0 then visit_id end) as visit_that_converted,
  count(distinct case when lv.converted_on_that_listing > 0 then visit_id end) as visits_that_converted_on_that_listing,
from 
  listing_views lv
inner join 
  reviews r using (listing_id)
group by all
order by 1 asc

------------------------------------------------------------------------------------
-- TESTING
------------------------------------------------------------------------------------
-- make sure conversion calcs work correctly
with agg as (
select
  v.platform,
	listing_id,
  a.visit_id,
  case when converted > 0 then 1 else 0 end as converted,
	count(a.visit_id) as listing_views,	
  sum(purchased_after_view) as purchases,
  case when purchased_after_view > 0 then 1 else 0 end as visit_purchased_after_view,
from 
  etsy-data-warehouse-prod.analytics.listing_views a 
inner join 
  etsy-data-warehouse-prod.weblog.visits v 
    on a.visit_id=v.visit_id
where 
  v._date >=current_date-30
  and a._date >=current_date-30
  and v.platform in ('mobile_web','desktop')
group by all
)
select * from agg where converted = 0 and visit_purchased_after_view = 1 limit 5
----------- how is that possible?
-- platform	listing_id	visit_id	converted	listing_views	purchases	visit_purchased_after_view
-- desktop	1557780880	ccKCO4aefYlz9o34L6LWetmSgyiN.1736533597516.1	0	3	3	1
-- desktop	1086521617	ccKCO4aefYlz9o34L6LWetmSgyiN.1736890788087.1	0	1	1	1
-- desktop	1738533251	b8ZvZxiMe2RN9L0xq3OSaZJZt5--.1737996090388.4	0	1	1	1
-- desktop	1852511435	3p9vnGnqaI3Nx-EWP4qhHAdHDSI8.1736476498030.1	0	1	1	1


--pulling out levels to check
select * from agg where listing_views > 1 order by converted_on_that_listing desc limit 10 
-- platform	listing_id	visit_id	converted_visit	total_listing_views	total_purchases	converted_on_that_listing
-- mobile_web	1582998659	DnfZ-tG4kaU7XwQru_VTKWxFCdD3.1737736368388.1	1	7	7	1
-- desktop	1843956467	7Sa_gF4vOJ68F1Rhj2LuoCYO8G0L.1736431214143.1	1	2	2	1
-- mobile_web	1623230017	D6OQ3yB09IDRUUMw922r9-vu3e67.1737493192911.1	1	3	3	1
-- mobile_web	1382527575	j94UvRUkyT9PrThSH1LlvTUpKP45.1738195371000.1	1	2	2	1
-- mobile_web	541787062	ob4uSoISMZ_IxWJyIZhhKhirETWf.1736459554529.1	1	3	3	1
-- desktop	1815984626	rJeBoiKoSPI1BGBrgIj9i32VPACm.1738254192817.1	1	2	2	1
-- mobile_web	1167562350	dEGp2FAs-EBfBDSG2OvTCNiOnHf3.1737839490853.1	1	3	3	1
-- desktop	1828961165	SL5FAbCBOXMMSEBjb7aivBV_ETfn.1738137880484.1	1	2	2	1
-- desktop	1702943333	JeDWKw0Vx-7hYSSIRUDWY0xEH0Os.1737777378350.1	1	2	2	1
-- mobile_web	1738713028	syEHhKxz-OU6CfmZQNor-FpaXZSL.1737838729566.3	1	2	2	1

----testing weird conversion stats
with agg as (
select
  v.platform,
	listing_id,
  a.visit_id,
  case when converted > 0 then 1 else 0 end as converted_visit,
	count(a.visit_id) as listing_views,	
  sum(purchased_after_view) as total_purchases,
  case when purchased_after_view > 0 then 1 else 0 end as converted_on_that_listing,
from 
  etsy-data-warehouse-prod.analytics.listing_views a 
inner join 
  etsy-data-warehouse-prod.weblog.visits v 
    on a.visit_id=v.visit_id
where 
  v._date >=current_date-30
  and a._date >=current_date-30
  and v.platform in ('mobile_web','desktop')
group by all
)
select 
  count(distinct case when converted_visit = 0 and converted_on_that_listing > 0 then visit_id end) as discrepancy,
  count(distinct case when converted_visit > 0 then visit_id end) as converted_visit,
  count(distinct case when converted_on_that_listing > 0 then visit_id end) as converted_on_that_listing,
  count(distinct visit_id),
from agg
-- discrepancy	converted_visit		converted_on_that_listing	unique_visits 
-- 96			10395691	9689489					455841677

-------testing platform counts
with both_platforms as (
select
  listing_id,
  count(distinct platform) as platform_count
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
group by all 
)
select 
  distinct listing_id
from both_platforms 
inner join etsy-data-warehouse-prod.analytics.listing_views using (listing_id)
where _date >= current_date-30
and platform in ('boe') 
and platform_count = 1
limit 5
--listings that are only viewed on boe listing_id
-- 1607582926
-- 272650094
-- 1286047855
-- 1486235298
-- 1575366713

-- select * from both_platforms where platform_count = 1 limit 10
-- -- listing_id	platform_count
-- -- 1412025910	1 ONLY MWEB
-- -- 1448983707	1 ONLY DESKTOP
-- -- 673802737	1
-- -- 989350892	1
-- -- 1741827753	1 ONLY MWEB
-- -- 1858863783	1
-- -- 623152638	1
-- -- 1249398379	1
-- -- 1857973045	1
-- -- 1800392717	1

select * from 
  etsy-data-warehouse-prod.analytics.listing_views
where _date >= current_date-30 and listing_id = 1575366713

--seeing shares of platform distro
with agg as (
select
  listing_id,
  count(distinct platform) as platform_count
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  platform in ('mobile_web','desktop')
  and _date >= current_date-30
group by all
)
select 
  count(distinct listing_id) as total_listings,
  count(distinct case when platform_count = 1 then listing_id end) / count(distinct listing_id) as one_platform_listing_share,
  count(distinct case when platform_count = 2 then listing_id end) / count(distinct listing_id) as two_platform_listing_share
from agg
-- total_listings	one_platform_listing_share	two_platform_listing_share
-- 72315490	0.5276031456054574	0.4723968543945426
