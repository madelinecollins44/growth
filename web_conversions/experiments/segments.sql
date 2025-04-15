/* VIEW LISTING MULTIPLE TIMES
-- browser is bucketed here if they viewed the same listing 1,2,3,4,5+ times in their bucketing visit 
*/

with unit_listing_views as (
    -- browser bucketed tests
    select 
      {{input_run_date}} as _date,
      split(lv.visit_id, ".")[0] as bucketing_id, 
      1 as bucketing_id_type, 
      lv.listing_id,
      count(lv.sequence_number)) as listing_views
    from `etsy-data-warehouse-prod.analytics.listing_views` lv
    where lv._date between DATE_SUB({{input_run_date}}, INTERVAL 14 DAY) and {{input_run_date}} 
    group by all
    union all 
    -- user bucketed_tests 
    select 
      {{input_run_date}} as _date,
      cast(v.user_id as string) as bucketing_id, 
      2 as bucketing_id_type, 
      lv.listing_id,
      count(lv.sequence_number)) as listing_views
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
      -- when recent_listing_views = 0 then '0' (default segment value will be applied)
      when max(listing_views) = 1 and '1'
      when max(listing_views) = 2 and '2'
      when max(listing_views) = 3 and '3'
      when max(listing_views) = 4 and '4'
     else '5+' end as segment_value
  from unit_listing_views


/* REVIEW ENGAGEMENT
-- bucketing for segment: 
------ 'engaged' : engaged w reviews, engaged w reviews + saw reviews
------ 'saw reviews' : saw reviews without engagement 
------ 'none' : did not see or engage with reviews

-- defintions: 
------ saw reviews: listing_page_reviews_seen
------ engage with reviews: listing_page_reviews_pagination, appreciation_photo_overlay_opened, listing_page_reviews_content_toggle_opened, shop_home_reviews_pagination, inline_appreciation_photo_click_shop_page 
*/
