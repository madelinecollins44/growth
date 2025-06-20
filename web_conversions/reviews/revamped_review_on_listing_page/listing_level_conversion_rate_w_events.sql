/* 
begin 
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
visit_id,
  listing_id,
  -- event counts 
  coalesce(count(case when event_name in ('view_listing') then sequence_number end),0) as listing_views,
  coalesce(count(case when event_name in ('listing_page_reviews_container_top_seen') then sequence_number end),0) as top_container_seen_events,
  coalesce(count(case when event_name in ('listing_page_reviews_seen') then sequence_number end),0) as review_seen_events,
  coalesce(count(case when event_name in ('listing_page_review_engagement_frontend') then sequence_number end),0) as review_engagements,
  coalesce(count(case when event_name in ('listing_page_reviews_pagination') then sequence_number end),0) as paginations,
  coalesce(count(case when event_name in ('appreciation_photo_overlay_opened') then sequence_number end),0) as photo_opens,
  coalesce(count(case when event_name in ('sort_reviews') then sequence_number end),0) as sort_reviews,
  coalesce(count(case when event_name in ('reviews_categorical_tag_clicked') then sequence_number end),0) as cat_tag_clicks,
-- event exists
  coalesce(max(case when event_name in ('view_listing') then 1 else 0 end),0) as has_lv,
  coalesce(max(case when event_name in ('listing_page_reviews_container_top_seen') then 1 else 0 end),0) as has_top_container,
  coalesce(max(case when event_name in ('listing_page_reviews_seen') then 1 else 0 end),0) as has_reviews_seen,
  coalesce(max(case when event_name in ('listing_page_review_engagement_frontend') then 1 else 0 end),0) as has_review_engagement,
  coalesce(max(case when event_name in ('listing_page_reviews_pagination') then 1 else 0 end),0) as has_pagination,
  coalesce(max(case when event_name in ('appreciation_photo_overlay_opened') then 1 else 0 end),0) as has_photo_opened,
 coalesce(max(case when event_name in ('sort_reviews') then 1 else 0 end),0) as has_review_sort,
 coalesce(max(case when event_name in ('reviews_categorical_tag_clicked') then 1 else 0 end),0) as has_cat_tag_click,
from 
  etsy-bigquery-adhoc-prod._scripta6046486fd948401dfc56a142dadc51795a80027.engagements
group by all 
)
, listing_purchases as (
select
  listing_id,
  visit_id,
  count(sequence_number) as views,
  count(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 1=1
  and platform in ('mobile_web','desktop')
  and _date >= current_date-4
  -- and _date between date('2025-06-10') and date('2025-06-24') -- two weeks after last reviews experiment was ramped 
group by all 
)
select -- listing view level 
  -- e.visit_id,
  -- e.listing_id,
  case when s.purchases > 0 then 1 else 0 end as purchased,
  coalesce(count(s.purchases),0) as total_purchases,
  count(distinct e.visit_id) as visits,
  coalesce(count(s.views),0) as lv,
  -- event counts 
  coalesce(sum(listing_views),0) as listing_views, 
  coalesce(sum(top_container_seen_events),0) as top_container_seen_events,
  coalesce(sum(review_seen_events),0) as review_seen_events,
  coalesce(sum(review_engagements),0) as review_engagements,
  coalesce(sum(paginations),0) as paginations,
  coalesce(sum(photo_opens),0) as photo_opens,
  coalesce(sum(sort_reviews),0) as sort_reviews,
  coalesce(sum(cat_tag_clicks),0) as cat_tag_clicks,
from 
  engagements e
left join 
  listing_purchases s
    on e.visit_id=s.visit_id
    and e.listing_id=cast(s.listing_id as string)  
group by all 


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
	visit_id,
  listing_id,
  -- event counts 
  coalesce(count(case when event_name in ('view_listing') then sequence_number end),0) as listing_views,
  coalesce(count(case when event_name in ('listing_page_reviews_container_top_seen') then sequence_number end),0) as top_container_seen_events,
  coalesce(count(case when event_name in ('listing_page_reviews_seen') then sequence_number end),0) as review_seen_events,
  coalesce(count(case when event_name in ('listing_page_review_engagement_frontend') then sequence_number end),0) as review_engagements,
  coalesce(count(case when event_name in ('listing_page_reviews_pagination') then sequence_number end),0) as paginations,
  coalesce(count(case when event_name in ('appreciation_photo_overlay_opened') then sequence_number end),0) as photo_opens,
  coalesce(count(case when event_name in ('sort_reviews') then sequence_number end),0) as sort_reviews,
  coalesce(count(case when event_name in ('reviews_categorical_tag_clicked') then sequence_number end),0) as cat_tag_clicks,
-- event exists
  coalesce(max(case when event_name in ('view_listing') then 1 else 0 end),0) as has_lv,
  coalesce(max(case when event_name in ('listing_page_reviews_container_top_seen') then 1 else 0 end),0) as has_top_container,
  coalesce(max(case when event_name in ('listing_page_reviews_seen') then 1 else 0 end),0) as has_reviews_seen,
  coalesce(max(case when event_name in ('listing_page_review_engagement_frontend') then 1 else 0 end),0) as has_review_engagement,
  coalesce(max(case when event_name in ('listing_page_reviews_pagination') then 1 else 0 end),0) as has_pagination,
  coalesce(max(case when event_name in ('appreciation_photo_overlay_opened') then 1 else 0 end),0) as has_photo_opened,
 coalesce(max(case when event_name in ('sort_reviews') then 1 else 0 end),0) as has_review_sort,
 coalesce(max(case when event_name in ('reviews_categorical_tag_clicked') then 1 else 0 end),0) as has_cat_tag_click,
from 
  etsy-bigquery-adhoc-prod._scripta6046486fd948401dfc56a142dadc51795a80027.engagements
group by all 
)
, listing_purchases as (
select
  listing_id,
  visit_id,
  count(sequence_number) as views,
  count(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 1=1
  and platform in ('mobile_web','desktop')
  and _date >= current_date-4
  -- and _date between date('2025-06-10') and date('2025-06-24') -- two weeks after last reviews experiment was ramped 
group by all 
)
select
  -- e.visit_id,
  e.listing_id,
  case when s.purchases > 0 then 1 else 0 end as purchased,
  coalesce(count(s.purchases),0) as total_purchases,
  count(distinct e.visit_id) as visits,
  coalesce(count(s.views),0) as lv,
  -- event counts 
  coalesce(sum(listing_views),0) as listing_views, 
  coalesce(sum(top_container_seen_events),0) as top_container_seen_events,
  coalesce(sum(review_seen_events),0) as review_seen_events,
  coalesce(sum(review_engagements),0) as review_engagements,
  coalesce(sum(paginations),0) as paginations,
  coalesce(sum(photo_opens),0) as photo_opens,
  coalesce(sum(sort_reviews),0) as sort_reviews,
  coalesce(sum(cat_tag_clicks),0) as cat_tag_clicks,
from 
  engagements e
left join 
  listing_purchases s
    on e.visit_id=s.visit_id
    and e.listing_id=cast(s.listing_id as string)  
group by all 
having visits = 40 
