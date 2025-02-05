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
having count(transaction_id) >= 5 and count(transaction_id) <= 100
order by 2 desc
)
, listing_views as (
select
  v.platform,
	listing_id,
  a.visit_id,
  case when converted > 0 then 1 else 0 end as converted,
	count(a.visit_id) as listing_views,	
  sum(purchased_after_view) as purchases,
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
  sum(purchases) as purchases,
  count(distinct case when lv.converted > 0 then visit_id end) as converted_visits
from 
  listing_views lv
inner join 
  reviews 
    on lv.listing_id=reviews.listing_id
group by all

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
