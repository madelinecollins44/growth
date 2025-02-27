------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- WHAT % OF SHOPS HAVE NULL SECTIONS? 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- begin 
-- create or replace temp table visited_shops as (
-- select
--   platform,
--   beacon.event_name, 
--   (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id, 
--   (select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id, 
--   visit_id, 
--   sequence_number,
-- from
--   `etsy-visit-pipe-prod.canonical.visit_id_beacons`
-- inner join 
--   etsy-data-warehouse-prod.weblog.visits using (visit_id)
-- where
--   date(_partitiontime) >= current_date-30
--   and _date >= current_date-30
--   and platform in ('mobile_web','desktop')
--   and (beacon.event_name in ('shop_home'))
-- group by all
-- );
-- end
-- etsy-bigquery-adhoc-prod._script146821e037627cbc101047258e54b02ce7ae2a33.visited_shops


with shop_visits as (
select
  shop_id,
  count(visit_id) as pageviews 
from 
  etsy-bigquery-adhoc-prod._script146821e037627cbc101047258e54b02ce7ae2a33.visited_shops
group by all 
)
, agg as (
select
  b.shop_id, -- active shops 
  shop_name,
  case when v.shop_id is not null then 1 else 0 end as visited,
  pageviews,
  case when s.shop_id is not null and active_listing_count > 0 then 1 else 0 end as has_sections_w_listings, 
  count(case when active_listing_count > 0 then name end) as number_of_sections_w_listings,
  sum(case when (name is null or name in ('')) and active_listing_count > 0 then 1 else 0 end) as empty_sections_with_listings,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
left join
  shop_visits v
    on v.shop_id=cast(b.shop_id as string) 
where 1=1
  and active_seller_status = 1 -- is an active seller 
group by all 
)
select
  count(distinct shop_id) as active_shops,
  sum(visited) as visited_shops,
  sum(pageviews) as pageviews,
  count(distinct case when empty_sections_with_listings > 0 then shop_id end) as active_shop_without_section_names,
  sum(case when empty_sections_with_listings > 0 then visited end) as visited_shop_without_section_names,
  sum(case when empty_sections_with_listings > 0 then pageviews end) as pageviews_without_section_names
from agg

-- sections by name 
select 
  case when (name is null or name in ('')) then 1 else 0 end as no_name,
  count(*)
from etsy-data-warehouse-prod.etsy_shard.shop_sections 
where active_listing_count > 0 
group by all 

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- TESTING 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/* TEST 1: check what im seeing in table with sections table */

-- check section #s
select * from etsy-data-warehouse-prod.etsy_shard.shop_sections 
where shop_id= 22114952 group by all 

-- check visit info 
select shop_id, count(visit_id) as pageviews 
from etsy-bigquery-adhoc-prod._script146821e037627cbc101047258e54b02ce7ae2a33.visited_shops
where shop_id in ('22114952')  group by all 
----ordered by # of sections
shop_id	shop_name	visited	pageviews	has_sections_w_listings	number_of_sections_w_listings	empty_sections_with_listings
22114952	RidhiSidhiBeads	1	576	1	20	0
8598186	DustedFindsEtsy	1	161	1	20	0
42229232	WildHerbStudioz	1	91	1	20	0
47575328	25andGoldVintage	1	334	1	20	0
12961441	PaddingPaws	1	2120	1	20	0
6887221	OscarsCreations	1	323	1	20	0
38677051	ChromaticInk	1	108	1	20	0
20858268	LegendaryApparelShop	1	456	1	20	0
54407659	DesignByTripleH	1	2302	1	20	0
11477140	lostinsounddetroit	1	49	1	20	0

-- ordered by # of sections, empty sections
shop_id	shop_name	visited	pageviews	has_sections_w_listings	number_of_sections_w_listings	empty_sections_with_listings
27855488	MLSvg	1	425	1	20	20
37991064	Abimodembroiderybis	1	167	1	20	4
15502339	PampillesAndCo	1	1820	1	20	20
15506298	ChiffonnierdePauline	1	194	1	20	20
38572135	Atelierdadri	1	199	1	20	20
18210007	Stoffhuzel	1	139	1	20	20
32913899	dalpassoshop	1	49	1	20	19
11279481	MotzisBunteWelt2014	1	786	1	20	20
18041715	BirgitHandarbeiten	1	48	1	20	20
15467598	WimAffiches	1	216	1	20	20
