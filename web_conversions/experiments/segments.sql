----------------------------------------------------------------------------------------------------------------------------------------------------------
-- VIEW LISTING MULTIPLE TIMES
----- Segmentation definition: a bucketed units repeat listing views (views of the same listing) in the last 14 days before bucketing
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
-- REVIEW ENGAGEMENT
----- bucketing for segment: 
------- 'engaged' : engaged w reviews, engaged w reviews + saw reviews (sort_reviews, listing_page_reviews_pagination, appreciation_photo_overlay_opened, listing_page_reviews_content_toggle_opened, shop_home_reviews_pagination, inline_appreciation_photo_click_shop_page )
------- 'saw reviews' : saw reviews without engagement 
------- 'none' : did not see or engage with reviews
----------------------------------------------------------------------------------------------------------------------------------------------------------
with review_engagements  as (
    -- browser bucketed tests
    select 
      {{input_run_date}} as _date,
      split(lv.visit_id, ".")[0] as bucketing_id, 
      1 as bucketing_id_type, 
      count(case when event_type in ('listing_page_reviews_seen','shop_home_reviews_section_top_seen') then sequence_number end) as review_seen_count,
      count(case when event_type in ('listing_page_reviews_pagination','appreciation_photo_overlay_opened','listing_page_reviews_content_toggle_opened''shop_home_reviews_pagination','inline_appreciation_photo_click_shop_page','sort_reviews') then sequence_number end) as review_engagement_count,
    from etsy-data-warehouse-prod.weblog.events e
    where lv._date between DATE_SUB({{input_run_date}}, INTERVAL 14 DAY) and {{input_run_date}} 
    and event_type in ('listing_page_reviews_pagination', 'appreciation_photo_overlay_opened','listing_page_reviews_content_toggle_opened','listing_page_reviews_seen' -- listing page events
                        'sort_reviews', -- event on both pages 
                        'shop_home_reviews_pagination','inline_appreciation_photo_click_shop_page','shop_home_reviews_section_top_seen')-- shop home events
    group by all
    union all 
    -- user bucketed_tests 
    select 
      {{input_run_date}} as _date,
      cast(v.user_id as string) as bucketing_id, 
      2 as bucketing_id_type, 
      count(case when event_type in ('listing_page_reviews_seen','shop_home_reviews_section_top_seen') then sequence_number end) as review_seen_count,
      count(case when event_type in ('listing_page_reviews_pagination','appreciation_photo_overlay_opened','listing_page_reviews_content_toggle_opened''shop_home_reviews_pagination','inline_appreciation_photo_click_shop_page','sort_reviews') then sequence_number end) as review_engagement_count,
    from etsy-data-warehouse-prod.weblog.events e
    where lv._date between DATE_SUB({{input_run_date}}, INTERVAL 14 DAY) and {{input_run_date}} 
    and event_type in ('listing_page_reviews_pagination', 'appreciation_photo_overlay_opened','listing_page_reviews_content_toggle_opened','listing_page_reviews_seen' -- listing page events
                        'sort_reviews', -- event on both pages 
                        'shop_home_reviews_pagination','inline_appreciation_photo_click_shop_page','shop_home_reviews_section_top_seen')-- shop home events
    group by all
)
select 
  _date,            
  bucketing_id, 
  bucketing_id_type,
  case 
     when review_engagement_count > then 'engaged with reviews'
     when review_seen_count > 0 then 'saw reviews'
     else 'undefined'
  end as segment_value,
  from unit_recent_listing_views
group by all 
