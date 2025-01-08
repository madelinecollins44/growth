--- get listing views + review seen events
with listing_events as (
select
	date(_partitiontime) as _date,
	visit_id,
  sequence_number,
	beacon.event_name as event_name,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
	date(_partitiontime) >= current_date-30
	and beacon.event_name in ("listing_page_reviews_seen","view_listing")
	-- and beacon.event_source in ('web')
group by all 
)
, ordered_events as (
select
	_date,
	visit_id,
	listing_id,
  sequence_number,
	event_name,
	lead(event_name) over (partition by visit_id, listing_id order by sequence_number) as next_event,
	lead(sequence_number) over (partition by visit_id, listing_id order by sequence_number) as next_sequence_number
from 
	listing_events
)
, listing_views as (
select
	visit_id,
	listing_id,
  sequence_number,
	case when next_event in ('listing_page_reviews_seen') then 1 else 0 end as saw_reviews
from
	ordered_events
where 
	event_name in ('view_listing')
)
, lv_stats as (
select
	lv.listing_id,
	case
  	when price_usd > 100 then 'high stakes'
  	else 'low stakes'
  end as listing_type,
	case when a.listing_id is null then 1 else 0 end as missing_in_analytics,
	count(lv.visit_id) as listing_views,
	count(a.visit_id) as a_listing_views,
	count(case when saw_reviews = 1 then lv.visit_id end) as views_and_reviews_seen,
	sum(case when saw_reviews = 1 then purchased_after_view end) as saw_reviews_and_purchased,
	sum(purchased_after_view) as purchases
from 
	listing_views lv
left join 
	etsy-data-warehouse-prod.analytics.listing_views a
		on lv.listing_id=cast(a.listing_id as string)
		and lv.visit_id=a.visit_id
		and lv.sequence_number=a.sequence_number		
where a._date >=current_date-30
group by all
)
, number_of_reviews as (
select
  _date,
  listing_id,
  max(listing_rating_count) as listing_rating_count,
   max(shop_rating_count) as shop_rating_count,
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
group by all
qualify row_number() over (partition by listing_id order by _date desc) = 1 -- takes # of reviews from day the query is run 
)
, review_score as (
select
  listing_id,
  sum(has_review) as reviews,
  sum(case when rating = 5 then 1 else 0 end) as count_5_star,
  sum(case when rating = 4 then 1 else 0 end) as count_4_star,
  sum(case when rating = 3 then 1 else 0 end) as count_3_star,
  sum(case when rating = 2 then 1 else 0 end) as count_2_star,
  sum(case when rating = 1 then 1 else 0 end) as count_1_star,
  avg(rating) as avg_rating
from `etsy-data-warehouse-prod.rollups.transaction_reviews` r
-- where date(review_date) >= "2023-01-01"
group by all 
)
select
  rv.listing_type,
	sum(missing_in_analytics) as errors,
  count(distinct rv.listing_id) as listings_viewed,
  sum(rv.listing_views) as listing_views,
  sum(rv.purchases) as purchases,
  sum(rv.saw_reviews_and_purchased) as saw_reviews_and_purchased,
  --listing page 
  sum(rv.views_and_reviews_seen) as views_and_reviews_seen,
  sum(n.listing_rating_count) as lp_listing_reviews,
  sum(n.shop_rating_count) as lp_shop_reviews,
  avg(n.listing_rating_count) as avg_lp_listing_reviews,
  avg(n.shop_rating_count) as avg_lp_shop_reviews,
  --transaction reviews 
  sum(reviews) as transaction_reviews,
  avg(reviews) as avg_transaction_reviews,
  sum(count_5_star) as count_5_star,
  sum(count_4_star) as count_4_star,
  sum(count_3_star) as count_3_star,
  sum(count_2_star) as count_2_star,
  sum(count_1_star) as count_1_star,
  avg(avg_rating) as avg_rating
from
  lv_stats rv
left join 
  number_of_reviews n 
		on rv.listing_id = cast(n.listing_id as string)
left join 
  review_score s 		
		on rv.listing_id = cast(s.listing_id as string)
group by all

	
------------------------------------------------------------------------
--TESTING WITH SECOND VERSION-- USING LEAD AND LAG VERSIONS
------------------------------------------------------------------------
--testing to see if lead functions work 
--- get listing views + review seen events
with listing_events as (
select
	date(_partitiontime) as _date,
	visit_id,
  sequence_number,
	beacon.event_name as event_name,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
	date(_partitiontime) >= current_date-2
	and beacon.event_name in ("listing_page_reviews_seen","view_listing")
group by all 
)
, ordered_events as (
select
	_date,
	visit_id,
	listing_id,
  sequence_number,
	event_name,
	lead(event_name) over (partition by visit_id, listing_id order by sequence_number) as next_event,
	lead(sequence_number) over (partition by visit_id, listing_id order by sequence_number) as next_sequence_number
from 
	listing_events
)
select * from ordered_events where visit_id in ('-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5') 

-- --B7vuSrm5eLGa9TuXE0r1OFKyJA.1736296649929.2
-- _date	visit_id	listing_id	sequence_number	event_name	next_event	sequence_number_1
-- 2025-01-08	--B7vuSrm5eLGa9TuXE0r1OFKyJA.1736296649929.2	1685439682	0	view_listing	listing_page_reviews_seen	48
-- 2025-01-08	--B7vuSrm5eLGa9TuXE0r1OFKyJA.1736296649929.2	1685439682	48	listing_page_reviews_seen		
-- 2025-01-08	--B7vuSrm5eLGa9TuXE0r1OFKyJA.1736296649929.2	1702050911	61	view_listing		
-- 2025-01-08	--B7vuSrm5eLGa9TuXE0r1OFKyJA.1736296649929.2	1695346331	152	view_listing		
-- 2025-01-08	--B7vuSrm5eLGa9TuXE0r1OFKyJA.1736296649929.2	1725311996	231	view_listing	listing_page_reviews_seen	269
-- 2025-01-08	--B7vuSrm5eLGa9TuXE0r1OFKyJA.1736296649929.2	1725311996	269	listing_page_reviews_seen		
-- 2025-01-08	--B7vuSrm5eLGa9TuXE0r1OFKyJA.1736296649929.2	1429997013	278	view_listing	listing_page_reviews_seen	321
-- 2025-01-08	--B7vuSrm5eLGa9TuXE0r1OFKyJA.1736296649929.2	1429997013	321	listing_page_reviews_seen		
-- 2025-01-08	--B7vuSrm5eLGa9TuXE0r1OFKyJA.1736296649929.2	1422615734	351	view_listing	listing_page_reviews_seen	405
-- 2025-01-08	--B7vuSrm5eLGa9TuXE0r1OFKyJA.1736296649929.2	1422615734	405	listing_page_reviews_seen		
-- 2025-01-08	--B7vuSrm5eLGa9TuXE0r1OFKyJA.1736296649929.2	1443683346	435	view_listing	listing_page_reviews_seen	507
-- 2025-01-08	--B7vuSrm5eLGa9TuXE0r1OFKyJA.1736296649929.2	1443683346	507	listing_page_reviews_seen		
-- 2025-01-08	--B7vuSrm5eLGa9TuXE0r1OFKyJA.1736296649929.2	1692586115	533	view_listing	listing_page_reviews_seen	581
-- 2025-01-08	--B7vuSrm5eLGa9TuXE0r1OFKyJA.1736296649929.2	1692586115	581	listing_page_reviews_seen		

-- -08ueSlE9jyRRjplTqFCiydkwQBX.1736283558876.1
-- _date	visit_id	listing_id	sequence_number	event_name	next_event	next_sequence_number
-- 2025-01-07	-08ueSlE9jyRRjplTqFCiydkwQBX.1736283558876.1	1848119305	13	view_listing	listing_page_reviews_seen	57
-- 2025-01-07	-08ueSlE9jyRRjplTqFCiydkwQBX.1736283558876.1	1848119305	57	listing_page_reviews_seen		

-- -0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5
-- _date	visit_id	listing_id	sequence_number	event_name	next_event	next_sequence_number
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1844365251	138	view_listing	listing_page_reviews_seen	172
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1844365251	172	listing_page_reviews_seen	view_listing	405
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1844365251	405	view_listing	listing_page_reviews_seen	447
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1844365251	447	listing_page_reviews_seen	view_listing	1177
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1844365251	1177	view_listing	listing_page_reviews_seen	1213
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1844365251	1213	listing_page_reviews_seen	view_listing	1640
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1844365251	1640	view_listing	listing_page_reviews_seen	1676
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1844365251	1676	listing_page_reviews_seen		
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1833255688	1129	view_listing	listing_page_reviews_seen	1161
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1833255688	1161	listing_page_reviews_seen	view_listing	1697
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1833255688	1697	view_listing	listing_page_reviews_seen	1729
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1833255688	1729	listing_page_reviews_seen		
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1844357515	314	view_listing	listing_page_reviews_seen	348
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1844357515	348	listing_page_reviews_seen	view_listing	635
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1844357515	635	view_listing	listing_page_reviews_seen	671
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1844357515	671	listing_page_reviews_seen	view_listing	1352
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1844357515	1352	view_listing	listing_page_reviews_seen	1386
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1844357515	1386	listing_page_reviews_seen	view_listing	1797
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1844357515	1797	view_listing	listing_page_reviews_seen	1833
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1844357515	1833	listing_page_reviews_seen		
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1832222844	688	view_listing	listing_page_reviews_seen	721
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1832222844	721	listing_page_reviews_seen	view_listing	1240
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1832222844	1240	view_listing	listing_page_reviews_seen	1275
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1832222844	1275	listing_page_reviews_seen	view_listing	1747
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1832222844	1747	view_listing	listing_page_reviews_seen	1780
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1832222844	1780	listing_page_reviews_seen		
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1845934869	2009	view_listing	listing_page_reviews_seen	2046
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1845934869	2046	listing_page_reviews_seen		
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1840268474	227	view_listing	listing_page_reviews_seen	264
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1840268474	264	listing_page_reviews_seen	view_listing	801
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1840268474	801	view_listing	listing_page_reviews_seen	838
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1840268474	838	listing_page_reviews_seen	view_listing	995
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1840268474	995	view_listing	listing_page_reviews_seen	1032
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1840268474	1032	listing_page_reviews_seen	view_listing	1483
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1840268474	1483	view_listing	listing_page_reviews_seen	1520
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1840268474	1520	listing_page_reviews_seen	view_listing	1533
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1840268474	1533	view_listing	listing_page_reviews_seen	1570
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1840268474	1570	listing_page_reviews_seen	view_listing	1871
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1840268474	1871	view_listing	listing_page_reviews_seen	1907
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1840268474	1907	listing_page_reviews_seen	view_listing	1956
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1840268474	1956	view_listing	listing_page_reviews_seen	1997
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1840268474	1997	listing_page_reviews_seen		
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1839894604	538	view_listing	listing_page_reviews_seen	575
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1839894604	575	listing_page_reviews_seen	view_listing	1067
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1839894604	1067	view_listing	listing_page_reviews_seen	1101
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1839894604	1101	listing_page_reviews_seen	view_listing	1589
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1839894604	1589	view_listing	listing_page_reviews_seen	1624
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1839894604	1624	listing_page_reviews_seen		
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1831117364	1424	view_listing	listing_page_reviews_seen	1457
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1831117364	1457	listing_page_reviews_seen		
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1830361894	1301	view_listing	listing_page_reviews_seen	1333
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1830361894	1333	listing_page_reviews_seen		
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1831090114	746	view_listing	listing_page_reviews_seen	778
-- 2025-01-08	-0d52jg8souWKPSkWkbfN8VNofOv.1736302322613.5	1831090114	778	listing_page_reviews_seen		
