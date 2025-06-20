with engagements as (
select
	date(_partitiontime) as _date,
	v.visit_id,
	-- v.sequence_number,
	beacon.event_name as event_name,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id,
  count(sequence_number) as actions
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` vb
inner join 
  etsy-data-warehouse-prod.weblog.visits v -- only looking at browsers in the experiment 
    on v.visit_id = vb.visit_id -- everything that happens on bucketing moment and after (cant do sequence number bc there is only one)
where 1=1
	-- and date(_partitiontime) between date('2025-03-01') and date('2025-03-15') -- two weeks before first reviews experiment was ramped 
  and platform in ('mobile_web','desktop')
  and _date >= current_date-4  
  and date(_partitiontime) >= current_date-4  
  -- and date(_partitiontime) between date('2025-06-10') and date('2025-06-24') -- two weeks after last reviews experiment was ramped 
  and beacon.event_name  in ('view_listing','listing_page_reviews_seen','listing_page_reviews_container_top_seen','listing_page_review_engagement_frontend','listing_page_reviews_pagination','appreciation_photo_overlay_opened','sort_reviews','reviews_categorical_tag_clicked')
  group by all 
)
, listing_purchases as (
select
  listing_id,
  visit_id,
  count(sequence_number) as views,
  sum(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 1=1
  and platform in ('mobile_web','desktop')
  and _date >= current_date-4
  -- and _date between date('2025-06-10') and date('2025-06-24') -- two weeks after last reviews experiment was ramped 
group by all 
)
select
  e.visit_id,
  e.listing_id,
  case when s.purchases > 0 then 1 else 0 end as purchased,
  sum(s.purchases) as total_purchases,
  sum(s.views) as lv,
  -- event counts 
  sum(case when event_name in ('view_listing') then actions end) as listing_views,
  sum(case when event_name in ('listing_page_reviews_container_top_seen') then actions end) as top_container_seen_events,
  sum(case when event_name in ('listing_page_reviews_seen') then actions end) as review_seen_events,
  sum(case when event_name in ('listing_page_review_engagement_frontend') then actions end) as review_engagements,
  sum(case when event_name in ('listing_page_reviews_pagination') then actions end) as paginations,
  sum(case when event_name in ('appreciation_photo_overlay_opened') then actions end) as photo_opens,
  sum(case when event_name in ('sort_reviews') then actions end) as sort_reviews,
  sum(case when event_name in ('reviews_categorical_tag_clicked') then actions end) as cat_tag_clicks,
-- event exists
  max(case when event_name in ('view_listing') then 1 else 0 end) as has_lv,
  max(case when event_name in ('listing_page_reviews_container_top_seen') then 1 else 0 end) as has_top_container,
  max(case when event_name in ('listing_page_reviews_seen') then 1 else 0 end) as has_reviews_seen,
  max(case when event_name in ('listing_page_review_engagement_frontend') then 1 else 0 end) as has_review_engagement,
  max(case when event_name in ('listing_page_reviews_pagination') then 1 else 0 end) as has_pagination,
  max(case when event_name in ('appreciation_photo_overlay_opened') then 1 else 0 end) as has_photo_opened,
  max(case when event_name in ('sort_reviews') then 1 else 0 end) as has_review_sort,
  max(case when event_name in ('reviews_categorical_tag_clicked') then 1 else 0 end) as has_cat_tag_click,
from 
  engagements e
left join 
  listing_purchases s
    on e.visit_id=s.visit_id
    and e.listing_id=cast(s.listing_id as string)  
group by all 
order by review_engagements desc limit 5





---------------------------------------------------------------------------------------------------------------------------------------------------
-- TESTING
---------------------------------------------------------------------------------------------------------------------------------------------------
/* begin
create or replace temp table engagements as (
select
	date(_partitiontime) as _date,
	v.visit_id,
	vb.sequence_number,
	beacon.event_name as event_name,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id,
  -- count(sequence_number) as actions
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` vb
inner join 
  etsy-data-warehouse-prod.weblog.visits v -- only looking at browsers in the experiment 
    on v.visit_id = vb.visit_id -- everything that happens on bucketing moment and after (cant do sequence number bc there is only one)
where 1=1
	-- and date(_partitiontime) between date('2025-03-01') and date('2025-03-15') -- two weeks before first reviews experiment was ramped 
  and platform in ('mobile_web','desktop')
  and _date >= current_date-4  
  and date(_partitiontime) >= current_date-4  
  -- and date(_partitiontime) between date('2025-06-10') and date('2025-06-24') -- two weeks after last reviews experiment was ramped 
  and beacon.event_name  in ('view_listing','listing_page_reviews_seen','listing_page_reviews_container_top_seen','listing_page_review_engagement_frontend','listing_page_reviews_pagination','appreciation_photo_overlay_opened','sort_reviews','reviews_categorical_tag_clicked')
  group by all 
);
end */

with engagements as (
select
	date(_partitiontime) as _date,
	v.visit_id,
	-- v.sequence_number,
	beacon.event_name as event_name,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id,
  count(sequence_number) as actions
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` vb
inner join 
  etsy-data-warehouse-prod.weblog.visits v -- only looking at browsers in the experiment 
    on v.visit_id = vb.visit_id -- everything that happens on bucketing moment and after (cant do sequence number bc there is only one)
where 1=1
	-- and date(_partitiontime) between date('2025-03-01') and date('2025-03-15') -- two weeks before first reviews experiment was ramped 
  and platform in ('mobile_web','desktop')
  and _date >= current_date-4  
  and date(_partitiontime) >= current_date-4  
  -- and date(_partitiontime) between date('2025-06-10') and date('2025-06-24') -- two weeks after last reviews experiment was ramped 
  and beacon.event_name  in ('view_listing','listing_page_reviews_seen','listing_page_reviews_container_top_seen','listing_page_review_engagement_frontend','listing_page_reviews_pagination','appreciation_photo_overlay_opened','sort_reviews','reviews_categorical_tag_clicked')
  group by all 
)
, listing_purchases as (
select
  listing_id,
  visit_id,
  count(sequence_number) as views,
  sum(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 1=1
  and platform in ('mobile_web','desktop')
  and _date >= current_date-4
  -- and _date between date('2025-06-10') and date('2025-06-24') -- two weeks after last reviews experiment was ramped 
group by all 
)
select
  e.visit_id,
  e.listing_id,
  case when s.purchases > 0 then 1 else 0 end as purchased,
  sum(s.purchases) as total_purchases,
  sum(s.views) as lv,
  -- event counts 
  sum(case when event_name in ('view_listing') then actions end) as listing_views,
  sum(case when event_name in ('listing_page_reviews_container_top_seen') then actions end) as top_container_seen_events,
  sum(case when event_name in ('listing_page_reviews_seen') then actions end) as review_seen_events,
  sum(case when event_name in ('listing_page_review_engagement_frontend') then actions end) as review_engagements,
  sum(case when event_name in ('listing_page_reviews_pagination') then actions end) as paginations,
  sum(case when event_name in ('appreciation_photo_overlay_opened') then actions end) as photo_opens,
  sum(case when event_name in ('sort_reviews') then actions end) as sort_reviews,
  sum(case when event_name in ('reviews_categorical_tag_clicked') then actions end) as cat_tag_clicks,
-- event exists
  max(case when event_name in ('view_listing') then 1 else 0 end) as has_lv,
  max(case when event_name in ('listing_page_reviews_container_top_seen') then 1 else 0 end) as has_top_container,
  max(case when event_name in ('listing_page_reviews_seen') then 1 else 0 end) as has_reviews_seen,
  max(case when event_name in ('listing_page_review_engagement_frontend') then 1 else 0 end) as has_review_engagement,
  max(case when event_name in ('listing_page_reviews_pagination') then 1 else 0 end) as has_pagination,
  max(case when event_name in ('appreciation_photo_overlay_opened') then 1 else 0 end) as has_photo_opened,
  max(case when event_name in ('sort_reviews') then 1 else 0 end) as has_review_sort,
  max(case when event_name in ('reviews_categorical_tag_clicked') then 1 else 0 end) as has_cat_tag_click,
from 
  engagements e
left join 
  listing_purchases s
    on e.visit_id=s.visit_id
    and e.listing_id=cast(s.listing_id as string)  
group by all 
order by review_engagements desc limit 5
/*
visit_id	listing_id	purchased	total_purchases	lv	listing_views	top_container_seen_events	review_seen_events	review_engagements	paginations	photo_opens	sort_reviews	cat_tag_clicks	has_lv	has_top_container	has_reviews_seen	has_review_engagement	has_pagination	has_photo_opened	has_review_sort	has_cat_tag_click
YYMX09v_7KQf0C0OmV7I8Ra0VIOO.1750120649699.3	1851745745	0	0	18	3	2	2302	1272	1164	108			1	1	1	1	1	1	0	0
LE3izwQTsJec6jnN_gx-R_d6oq8c.1750152906953.1	1247525345	0	0	12	2	2	222	768	529	239			1	1	1	1	1	1	0	0
ZB8CyqN0Z_MG4uBAH-KBE9OERMVT.1750213375955.2	492940138	0	0	10	2	2	544	434	418				1	1	1	1	1	0	0	0
psFGXdprU82ws5PnP59QaJbQPt8t.1750304579704.3	1571697436	0	0	5	1	1	623	421	402				1	1	1	1	1	0	0	0
O0Kc_shIQmzr9qYFfeRKRwJMiVdI.1750369302785.1	1664073282	0	0	12	2	2	204	364	356	8			1	1	1	1	1	1	0	0
*/
