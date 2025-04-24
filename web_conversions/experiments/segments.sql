/* link to segmentation checks in BQ: https://docs.google.com/spreadsheets/d/19KuDMYtx9ydrVQ0d6Bmv5Zo1z14_FEvcxxwEoCs7vLc/edit?gid=2031585801#gid=2031585801 */

----------------------------------------------------------------------------------------------------------------------------------------------------------
/* VIEW LISTING MULTIPLE TIMES
Segmentation definition: a bucketed units repeat listing views (views of the same listing) in the last 14 days before bucketing

etsy-data-warehouse-dev.catapult_temp.segmentation_sample_run_repeat_listing_views_1744910606 
*/
----------------------------------------------------------------------------------------------------------------------------------------------------------
-- segmentations 
with unit_recent_listing_views as (
    -- browser bucketed tests
    select 
      {{input_run_date}} as _date,
      split(lv.visit_id, ".")[0] as bucketing_id, 
      1 as bucketing_id_type, 
      listing_id,
      count(distinct concat(lv.visit_id, lv.sequence_number)) as recent_listing_views
    from `etsy-data-warehouse-prod.analytics.listing_views` lv
    where lv._date between DATE_SUB({{input_run_date}}, INTERVAL 14 DAY) and {{input_run_date}} 
    group by all
    union all 
    -- user bucketed_tests 
    select 
      {{input_run_date}} as _date,
      cast(v.user_id as string) as bucketing_id, 
      2 as bucketing_id_type, 
      listing_id,
      count(distinct concat(lv.visit_id, lv.sequence_number)) as recent_listing_views
    from `etsy-data-warehouse-prod.analytics.listing_views` lv
    left join `etsy-data-warehouse-prod.weblog.visits` v 
      on v.visit_id = lv.visit_id
    where lv._date between DATE_SUB({{input_run_date}}, INTERVAL 14 DAY) and {{input_run_date}} 
    and v._date = {{input_run_date}}
    group by all
)
select 
  _date,            
  bucketing_id, 
  bucketing_id_type,
  case 
    when max(recent_listing_views) = 1 then '1'
    when max(recent_listing_views) = 2 then '2'
    when max(recent_listing_views) = 3 then '3'
    when max(recent_listing_views) = 4 then '4'
    else '5+' end as segment_value
  from unit_recent_listing_views
group by all 

------TESTING
with unit_listing_views as (
    -- browser bucketed tests
    select 
      _date,
      split(lv.visit_id, ".")[0] as bucketing_id, 
      1 as bucketing_id_type, 
      lv.listing_id,
      count(lv.sequence_number) as listing_views
    from `etsy-data-warehouse-prod.analytics.listing_views` lv
    where lv._date between DATE_SUB(current_date, INTERVAL 14 DAY) and current_date 
    group by all
    union all 
    -- user bucketed_tests 
    select 
      lv._date,
      cast(v.user_id as string) as bucketing_id, 
      2 as bucketing_id_type, 
      lv.listing_id,
      count(lv.sequence_number) as listing_views
    from `etsy-data-warehouse-prod.analytics.listing_views` lv
    left join `etsy-data-warehouse-prod.weblog.visits` v 
      on v.visit_id = lv.visit_id
    where lv._date between DATE_SUB(current_date, INTERVAL 14 DAY) and current_date 
    and v._date = current_date
    group by all
)
, agg as (
select 
  _date,            
  bucketing_id, 
  bucketing_id_type,
  case 
    when max(listing_views) = 0 then '0' 
    when max(listing_views) = 1 then '1'
    when max(listing_views) = 2 then '2'
    when max(listing_views) = 3 then '3'
    when max(listing_views) = 4 then '4'
    else '5+' end as segment_value
  from unit_listing_views
group by all 
)
select 
  bucketing_id,
  segment_value
from agg 
where segment_value is not null
QUALIFY ROW_NUMBER() OVER (PARTITION BY segment_value ORDER BY RAND()) = 1
LIMIT 5
/* bucketing_id	segment_value
MsUnXKe1w4CAtqR5Ho2G8kWhyFC2	1
E887B3FB031D47B083C19923A2A8	5+
wjiWrNJIQWGVdM9TxBy80A	2
kkabya_0-v1S9gi94optH3VEwVR2	4
D51480042CED43C0A000419842B9	3 */


----------------------------------------------------------------------------------------------------------------------------------------------------------
/* REVIEW ENGAGEMENT
Segment definition: a bucketed units listing and shop home page review engagements in the last 14 days before bucketing. engagements include: paginating, sorting, opening a review photo, and expanding reviews. 
-- bucketing for segment: 
-- 'engaged' : engaged w reviews, engaged w reviews + saw reviews (sort_reviews, listing_page_reviews_pagination, appreciation_photo_overlay_opened, listing_page_reviews_content_toggle_opened, shop_home_reviews_pagination, inline_appreciation_photo_click_shop_page )
-- 'saw reviews' : saw reviews without engagement 
-- 'none' : did not see or engage with reviews

etsy-data-warehouse-dev.catapult_temp.segmentation_sample_run_review_engagement_last_14_days_1744913439
*/
----------------------------------------------------------------------------------------------------------------------------------------------------------
with review_engagements  as (
    -- browser bucketed tests
    select 
      {{input_run_date}} as _date,
      split(visit_id, ".")[0] as bucketing_id, 
      1 as bucketing_id_type, 
      count(case when event_type in ('listing_page_reviews_container_top_seen','shop_home_reviews_section_top_seen') then sequence_number end) as review_seen_count,
      count(case when event_type in ('listing_page_reviews_pagination','appreciation_photo_overlay_opened','listing_page_reviews_content_toggle_opened','shop_home_reviews_pagination','inline_appreciation_photo_click_shop_page','sort_reviews') then sequence_number end) as review_engagement_count,
    from etsy-data-warehouse-prod.weblog.events 
    where _date between DATE_SUB({{input_run_date}}, INTERVAL 14 DAY) and {{input_run_date}} 
    and event_type in ('listing_page_reviews_pagination', 'appreciation_photo_overlay_opened','listing_page_reviews_content_toggle_opened','listing_page_reviews_container_top_seen' ,'sort_reviews','shop_home_reviews_pagination','inline_appreciation_photo_click_shop_page','shop_home_reviews_section_top_seen')
    group by all
    union all 
    -- user bucketed_tests 
    select 
      {{input_run_date}} as _date,
      cast(user_id as string) as bucketing_id, 
      2 as bucketing_id_type, 
      count(case when event_type in ('listing_page_reviews_container_top_seen','shop_home_reviews_section_top_seen') then sequence_number end) as review_seen_count,
      count(case when event_type in ('listing_page_reviews_pagination','appreciation_photo_overlay_opened','listing_page_reviews_content_toggle_opened','shop_home_reviews_pagination','inline_appreciation_photo_click_shop_page','sort_reviews') then sequence_number end) as review_engagement_count,
   from etsy-data-warehouse-prod.weblog.events 
    where _date between DATE_SUB({{input_run_date}}, INTERVAL 14 DAY) and {{input_run_date}} 
    and event_type in ('listing_page_reviews_pagination', 'appreciation_photo_overlay_opened','listing_page_reviews_content_toggle_opened','listing_page_reviews_container_top_seen' ,'sort_reviews','shop_home_reviews_pagination','inline_appreciation_photo_click_shop_page','shop_home_reviews_section_top_seen')
    group by all
)
  select 
   _date,            
   bucketing_id, 
   bucketing_id_type,
    case 
      when review_engagement_count > 0 then 'engaged_with_reviews'
      when review_seen_count > 0 then 'only_saw_reviews'
      else 'undefined'
   end as segment_value,
  from review_engagements
  group by all

----- TESTING
-- select * from etsy-bigquery-adhoc-prod._scriptc52539c2284ac4359b2932a9a528ef9065a91f38.review_engagement_segment QUALIFY ROW_NUMBER() OVER (PARTITION BY segment_value ORDER BY RAND()) = 1
/* _date	bucketing_id	bucketing_id_type	segment_value
2025-04-03	QUspz7Nlk8aICOmX1ztcLtbGV6fZ	1	engaged_with_reviews
2025-04-04	ECfdianQvQP4Bd7HLtj0yxIeZEQN	1	saw_reviews
2025-04-15	Lqwo653Nf5KND3HaLa4p8B2W_Mmd	1	engaged_with_reviews
2025-04-14	323881676	2	saw_reviews
2025-04-15	At2JFVo5hq7bP-XXxM8xAWy4wP7O	1	engaged_with_reviews
2025-04-03	2Aemqa6r3beHPDvQuUbt2yr9DtRA	1	saw_reviews
*/

/* bucketing ids with both -- these should be engaged with reviews 
_date	bucketing_id
2025-04-09	32147677
2025-04-09	107608699
2025-04-09	236775088
2025-04-09	18688294
2025-04-03	8bCI7N754tO6dgkMULimvi_kTGvW	
2025-04-03	GPQ9ATGh7ewTvALMKzePjs49rjuN	
2025-04-14	azTuIy6RGQ-JpemcpqQxZ2yT1TwE	
2025-04-14	aQMzQBS6NCf8S5ra98tVMYIWHzH0	
*/

select * from etsy-bigquery-adhoc-prod._scriptc52539c2284ac4359b2932a9a528ef9065a91f38.review_engagement_segment where bucketing_id  in ('8bCI7N754tO6dgkMULimvi_kTGvW') and _date in ('2025-04-03') 

select event_type, count(sequence_number),count(distinct concat(visit_id, sequence_number)) from etsy-data-warehouse-prod.weblog.events 
where 1=1
  -- and user_id = 32147677
  and split(visit_id, ".")[0] in ('8bCI7N754tO6dgkMULimvi_kTGvW') 
  and _date in ('2025-04-03')
  and event_type in ('listing_page_reviews_pagination', 'appreciation_photo_overlay_opened','listing_page_reviews_content_toggle_opened','listing_page_reviews_container_top_seen' -- listing page events
                      'sort_reviews', -- event on both pages 
                      'shop_home_reviews_pagination','inline_appreciation_photo_click_shop_page','shop_home_reviews_section_top_seen')-- shop home events
group by all

select segment_value, count(distinct bucketing_id) from etsy-bigquery-adhoc-prod._scriptc52539c2284ac4359b2932a9a528ef9065a91f38.review_engagement_segment group by all 
-- segment_value	f0_
-- saw_reviews	9532574
-- engaged_with_reviews	10397555

----------------------------------------------------------------------------------------------------------------------------------------------------------
/* SELLER TIER
Segmentation definition: the seller tier of the shop home page a unit is bucketed on

etsy-data-warehouse-dev.catapult_temp.segmentation_sample_run_shop_home_seller_tier_1745259293
*/
----------------------------------------------------------------------------------------------------------------------------------------------------------
with shop_ids as (
select
  date(_partitiontime) as _date,
  beacon.event_name,
  beacon.browser_id as bucketing_id,
  1 as bucketing_id_type, 
  sequence_number,
  visit_id, 
  (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id,
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
  date(_partitiontime) = {{ input_run_date }} 
  and beacon.event_name in ('shop_home')
group by all
union all
select
  date(_partitiontime) as _date,
  beacon.event_name,
  cast(beacon.user_id as string) as bucketing_id,
  2 as bucketing_id_type, 
  sequence_number,
  visit_id, 
  (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id,
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
  date(_partitiontime) = {{ input_run_date }} 
  and beacon.event_name in ('shop_home')
group by all
)
, shop_tier as (
select
  si.*,
  replace(sb.seller_tier_new, ' ', '_') as segment_value
from 
  shop_ids si
left join 
  etsy-data-warehouse-prod.rollups.seller_basics sb 
    on si.shop_id=cast(sb.shop_id as string)
where 1=1
  and active_seller_status = 1 -- active sellers
  and is_frozen = 0  -- not frozen accounts 
  and active_listings > 0 -- shops with active listings
)
select
  _date,
  bucketing_id,
  bucketing_id_type,
  visit_id,
  sequence_number,
  segment_value
from 
  shop_tier


----- TESTING
select 
  count(distinct bucketing_id) as buckets,
  segment_value
from 
  shop_tier
group by all 
order by 1 desc 
/*
buckets	segment_value
816072	Top Shop
671027	Power Shop
437393	Medium Shop
248061	Small Shop
99797	Listed Shop
811	Closed Shop
*/

-- select bucketing_id_type, count(distinct bucketing_id) from etsy-data-warehouse-dev.catapult_temp.segmentation_sample_run_shop_home_seller_tier_1745259293 group by all
-- bucketing_id_type	f0_
-- 1	20841564
-- 2	6047575

-- SELECT  
--   segment_value, 
--   COUNT(*) AS total_bucketing_units,
--   COUNT(*) / (SELECT COUNT(*) FROM `etsy-data-warehouse-dev.catapult_temp.segmentation_sample_run_shop_home_seller_tier_1745259293`) AS perc_bucketing_units
-- FROM `etsy-data-warehouse-dev.catapult_temp.segmentation_sample_run_shop_home_seller_tier_1745259293` 
-- GROUP BY 1
-- ORDER BY 2 DESC
-- segment_value	total_bucketing_units	perc_bucketing_units
-- undefined	82100422	0.97641034464931609
-- Top_Shop	688545	0.0081887820352076063
-- Power_Shop	504530	0.00600031399577848
-- Medium_Shop	395883	0.0047081884240595647
-- Small_Shop	278752	0.0033151636710428375
-- Listed_Shop	115221	0.0013703093550583559
-- Closed_Shop	580	6.8978695370969382e-06


WITH experiment_bucketing_units AS (
  SELECT *
  FROM `etsy-data-warehouse-dev.catapult_temp.segmentation_sample_run_shop_home_seller_tier_1745259293`
    INNER JOIN `etsy-data-warehouse-prod.catapult_unified.bucketing_period` USING(_date, bucketing_id, bucketing_id_type, bucketing_ts)
  WHERE experiment_id = 'growth_regx.sh_search_bar_redesign_mweb'
)

SELECT  
  segment_value, 
  COUNT(*) AS total_bucketing_units,
  COUNT(*) / (SELECT COUNT(*) FROM experiment_bucketing_units)
FROM experiment_bucketing_units
GROUP BY 1
ORDER BY 2 DESC
/* segment_value	total_bucketing_units	f0_
Top_Shop	274703	0.3202719308677211
Power_Shop	203081	0.2367689613602606
Medium_Shop	169478	0.19759174926957346
Small_Shop	121101	0.14118976166991948
Listed_Shop	45032	0.052502104421266661
undefined	44317	0.051668497105109137
Closed_Shop	6	6.9953061495736361e-06
*/

WITH experiment_bucketing_units AS (
  SELECT *
  FROM `etsy-data-warehouse-dev.catapult_temp.segmentation_sample_run_shop_home_seller_tier_1745259293`
    INNER JOIN `etsy-data-warehouse-prod.catapult_unified.bucketing_period` USING(_date, bucketing_id, bucketing_id_type, bucketing_ts)
  WHERE experiment_id = 'growth_regx.sh_search_bar_redesign_mweb'
)

SELECT  
  segment_value, 
  COUNT(*) AS total_bucketing_units,
  COUNT(*) / (SELECT COUNT(*) FROM experiment_bucketing_units)
FROM experiment_bucketing_units
GROUP BY 1
ORDER BY 2 DESC

select _date, count(distinct bucketing_id) from etsy-data-warehouse-prod.catapult_unified.bucketing_period where experiment_id = 'growth_regx.sh_search_bar_redesign_mweb' and (_date = ('2025-04-20') or _date in ('2025-04-19')) group by all 
-- _date	f0_
-- 2025-04-19	2124754
-- 2025-04-20	2982472
----------------------------------------------------------------------------------------------------------------------------------------------------------
/* MFTS MODULE TYPE
Segmentation definition: the more from this shop module layout a a bucketed unit saw

Shop segmentation: 
	has 5+ listings (since thats the space allowed) and 2+ sections
	has <5 listings and 2+ sections
	has 6+ listings and no sections
	has <6 listings and no sections

Purpose: so basically we are redesigning the experience to only show listings - no sections. we're also adding in seller info so essentially the listings are a secondary priority in the module. 
	because the treatment will be universal (seller info plus max 6 listings) we need a way to kind of see what that change means for different shops - 
	so the impact may be different if a shop only had 2 listings vs a shop that has 5 listings and sections

*/
----------------------------------------------------------------------------------------------------------------------------------------------------------
with listing_views as (
select
  _date,
  listing_id,
  sequence_number,
  visit_id,
  seller_user_id
from
  etsy-data-warehouse-prod.analytics.listing_views
where _date >= current_date - 1
) 
, seller_inventory as (
select 
  b.user_id as seller_user_id,
  shop_name,
  active_listings,
  count(s.id) as sections,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
where
  active_seller_status = 1 -- active sellers
  and is_frozen = 0  -- not frozen accounts 
  and active_listings > 0 -- shops must have some listings 
group by all
)
, agg as (
select
  _date,
  visit_id,
  sequence_number,
  seller_user_id,
  coalesce(active_listings,0) as active_listings,
  coalesce(sections,0) as sections,
  coalesce(concat(active_listings, '-', sections),'0') as value,
  case 
    when active_listings >= 5 and sections >= 2 then '5_plus_listings_2_plus_sections'
    when active_listings < 5 and sections >= 2 then 'less_than_5_listings_2_plus_sections'
    when active_listings >= 6 and sections = 0 then '6_plus_listings_no_sections'
    when active_listings < 6 and sections = 0 then 'less_than_6_listings_no_sections'
    when sections = 1 then '1_section'
    else 'other'
  end as segment_value
from 
  listing_views
left join 
  seller_inventory using (seller_user_id)
)
select segment_value, count(sequence_number) from agg  group by all order by 2 asc limit 10 



------ TESTING
-- TEST 1: what % of active sellers do not have any listings?
with agg as (
select 
  b.user_id as seller_user_id,
  active_listings,
  -- count(s.id) as sections,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
-- left join 
--   etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
where
  active_seller_status = 1 -- active sellers
  and is_frozen = 0  -- not frozen accounts 
  -- and active_listings > 0 -- shops must have some listings 
group by all
)
select active_listings, count(distinct seller_user_id) from agg group by all order by 1 asc 

-- TEST 2: how many of the visited shops dont have any listings?
select
  active_listings,
  count(distinct seller_user_id) as sellers,
  count(sequence_number) as listing_views
from
  etsy-data-warehouse-prod.analytics.listing_views lv
inner join 
    etsy-data-warehouse-prod.rollups.seller_basics sb
      on lv.seller_user_id=sb.user_id
where 
  _date >= current_date - 1
  and active_seller_status = 1 -- active sellers
  and is_frozen = 0  -- not frozen accounts 
group by all
order by 1 asc

-- TEST 3: what % of listing views are from null sellers? 
select
  count(distinct seller_user_id) as sellers,
  sum(case when seller_user_id is null then 1 else 0 end) as null_sellers,
  count(sequence_number) as listing_views,
  count(case when seller_user_id is null then sequence_number end) as null_lv,
from
  etsy-data-warehouse-prod.analytics.listing_views lv
where 
  _date >= current_date - 1
group by all
order by 1 asc


-- TEST 4: make sure totals add up across CTEs
with listing_views as (
select
--  {{input_run_date}} as _date,
  listing_id,
  sequence_number,
  visit_id,
  seller_user_id
from
  etsy-data-warehouse-prod.analytics.listing_views
where _date >= current_date - 1
) 
-- views	sellers
-- 73034115	1070206
, seller_inventory as (
select 
  b.user_id as seller_user_id,
  shop_name,
  active_listings,
  count(s.id) as sections,
  coalesce(concat(active_listings, '-', count(s.id)),'N/A') as listing_section_combo,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
-- where
--   active_seller_status = 1 -- active sellers
--   and is_frozen = 0  -- not frozen accounts 
--   and active_listings > 0 -- shops must have some listings 
group by all
)
, agg as (
select
  -- _date,
  visit_id,
  sequence_number,
  seller_user_id,
  listing_section_combo,
  case 
    -- everything with 2+ sections
    when active_listings >= 5 and sections >= 2 then '5_plus_listings_2_plus_sections'
    when active_listings < 5 and sections >= 2 then 'less_than_5_listings_2_plus_sections'
    -- everything with no sections
    when active_listings >= 6 and sections = 0 then '6_plus_listings_no_sections'
    when active_listings < 6 and sections = 0 then 'less_than_6_listings_no_sections'
    when sections = 1 then '1_section'
    else 'other'
  end as segment_value
from 
  listing_views
left join 
  seller_inventory using (seller_user_id)
)
select count(distinct seller_user_id) as sellers, count(sequence_number) as sequence_number from agg 
-- sellers	sequence_number
-- 1070206	73034115

-- TEST 5: confirm against listing_side module deliveries 
with mfts_deliveries as (
select
	date(_partitiontime) as _date,
	visit_id,
  sequence_number,
  coalesce(count(distinct(select value from unnest(beacon.properties.key_value) where key = "section_ids")),0) as sections,
  coalesce(count(distinct(select value from unnest(beacon.properties.key_value) where key = "listing_ids")),0) as listings,
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
inner join 
  etsy-data-warehouse-prod.weblog.visits using (visit_id)
where
	date(_partitiontime) >= current_date-1
  and _date >= current_date-1
  and (beacon.event_name in ("recommendations_module_seen") and (select value from unnest(beacon.properties.key_value) where key = "module_placement") in ("listing_side")) -- MFTS modules 
group by all 
)
select
  case 
    -- everything with 2+ sections
    when listings = 5 and sections = 2 then '5_plus_listings_2_plus_sections'
    when listings = 6 and sections = 0 then '6_plus_listings_no_sections'
    else 'other'
  end as segment_value,
  count(sequence_number) as views
from 
  mfts_deliveries
group by all 

