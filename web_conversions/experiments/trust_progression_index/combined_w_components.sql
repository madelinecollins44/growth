BEGIN
------------------------- PULL ALL KEY METRICS
create or replace temp table key_metrics as (
with experiments as (
select 
  launch_id,
  end_date, 
  start_date,
  config_flag, 
  status,
  ramp_decision,
  platform,
  subteam,
  group_name,
  initiative
from 
  etsy-data-warehouse-prod.rollups.experiment_reports 
where 1=1
  and (trim(lower(initiative)) like '%drive conversion%')
  and end_date >= '2025-03-01'
  and (lower(platform) like ('%boe ios%') or lower(platform) like ('%boe android%') or lower(platform) like ('%desktop%') or lower(platform) like ('%mweb%'))
  )
, coverages as (
select 
  e.launch_id,
  max(case when coverage_name in ('GMS coverage') then coverage_value end) as gms_coverage,
  max(case when coverage_name in ('Traffic coverage') then coverage_value end) as traffic_coverage,
from 
  etsy-data-warehouse-prod.catapult.results_coverage_day rcd
inner join 
  experiments e
   on rcd.launch_id=e.launch_id
   and rcd._date=e.end_date
where 1=1
  and coverage_name in ('GMS coverage','Traffic coverage')
  and unit in ('PERCENTAGE')
  and lower(segmentation) in ('any')
  and lower(segment) in('all')
group by all
)
, metrics as (
select
  e.launch_id,
  boundary_start_sec,
  metric_variant_name,
 coalesce(dense_rank() over(partition by e.launch_id, boundary_start_sec order by metric_variant_name asc),0)as variant_rnk,

  max(case when metric_display_name in ('Ads Conversion Rate') then metric_value_control end) as ads_cr_control,
  max(case when metric_display_name in ('Ads Conversion Rate') then metric_value_treatment end) as ads_cr_treatment,
  max(case when metric_display_name in ('Ads Conversion Rate') then relative_change end) as ads_cr_change,
  max(case when metric_display_name in ('Ads Conversion Rate') then p_value end) as ads_cr_pvalue,

  max(case when metric_display_name in ('GMS per Unit') then metric_value_control end) as gpu_control,
  max(case when metric_display_name in ('GMS per Unit') then metric_value_treatment end) as gpu_treatment,
  max(case when metric_display_name in ('GMS per Unit') then relative_change end) as gpu_change,
  max(case when metric_display_name in ('GMS per Unit') then p_value end) as gpu_pvalue,

  max(case when metric_display_name in ('Conversion Rate') then metric_value_control end) as cr_control,
  max(case when metric_display_name in ('Conversion Rate') then metric_value_treatment end) as cr_treatment,
  max(case when metric_display_name in ('Conversion Rate') then relative_change end) as cr_change,
  max(case when metric_display_name in ('Conversion Rate') then p_value end) as cr_pvalue,
from 
  etsy-data-warehouse-prod.catapult.results_metric_day rmd
inner join 
  experiments e
   on rmd.launch_id=e.launch_id
   and rmd._date=e.end_date
where 1=1
  and segmentation in ('any')
  and segment in ('all')
  and metric_id in (
      1029227163677, -- CR
      1275588643427, -- GMS per Unit
      1227229423992 -- Ads CR
  )
group by all )
select
  e.*,
  metric_variant_name,
  variant_rnk,
  gms_coverage,
  traffic_coverage,
  ads_cr_control,
  ads_cr_treatment,
  ads_cr_change,
  ads_cr_pvalue,
  case when ads_cr_pvalue <= 0.05 then 1 else 0 end as ads_cr_sig,
  gpu_control,
  gpu_treatment,
  gpu_change,
  gpu_pvalue,
  case when gpu_pvalue <= 0.05 then 1 else 0 end as gpu_sig,
  cr_control,
  cr_treatment,
  cr_change,
  cr_pvalue,
  case when cr_pvalue <= 0.05 then 1 else 0 end as cr_sig,
 from 
  experiments e
inner join
  metrics mtcs
   on mtcs.launch_id=e.launch_id
inner join 
  coverages cvg
    on cvg.launch_id=e.launch_id
order by  end_date, metric_variant_name asc
);

------------------------- PULL ALL TRUST METRICS 
create or replace temp table trust_measurements as (
with experiments as (
select 
  launch_id,
  end_date, 
  config_flag, 
  status,
  start_date,
  ramp_decision,
  platform,
  subteam,
  group_name,
  initiative,
  gms_coverage,
  traffic_coverage,
  ads_cr_control,
  gpu_control,
  cr_control,
from 
  key_metrics
)
select
  launch_id,
  end_date, 
  config_flag, 
  status,
  start_date,
  ramp_decision,
  platform,
  subteam,
  group_name,
  initiative,
  gms_coverage,
  traffic_coverage,
  ads_cr_control,
  gpu_control,
  cr_control,
  variant_id,
  /* TRUST BUILDING AGGS */
   sum(case when event_id in ('view_listing') then coalesce(event_value,filtered_event_value) else null end) as view_listing_actions,
 sum(case when event_id in ('listing_expand_description_open','product_details_content_toggle_open','listing_item_details_read_description_clicked','listing_item_details_read_more_description_tapped') then coalesce(event_value,filtered_event_value) else null end) as open_description_count,
 sum(case when event_id in ('listing_page_image_carousel_changed','image_carousel_swipe','appreciation_photo_carousel_thumbnails_pressed_next_listing_page') then coalesce(event_value,filtered_event_value) else null end) as image_scrolling_count,
 sum(case when event_id in ('shop_home') then coalesce(event_value,filtered_event_value) else null end) as shop_home_count,
  sum(case when event_id in ('cart_view') then coalesce(event_value,filtered_event_value) else null end) as cart_view_count,
  sum(case when event_id in ('search') then coalesce(event_value,filtered_event_value) else null end) as search_count,
   sum(case when event_id in ('listing_page_review_engagement_frontend', -- web: listing engagement 
      'listing_see_all_reviews_tapped', --iOS: review engagement 
      'listing_screen_review_card_swipe', --iOS: review engagement 
      'review_card_tapped',--iOS: review engagement 
      'review_updates_view_shop_home_reviews', --iOS: review engagement 
      'listing_screen_reviews_seen',--iOS: review engagement 
      'fullscreen_review_media_screen', --iOS: review engagement 
      'reviews_sort_suggested_clicked', --iOS: review engagement 
      'reviews_sort_most_recent_clicked', --iOS: review engagement 
      'reviews_sort_highest_rated_clicked', --iOS: review engagement 
      'reviews_sort_lowest_rated_clicked', --iOS: review engagement  
      'listing_image_swipe',--iOS: review engagement 
      'see_all_reviews_clicked', --Android: review engagement 
      'listing_reviews_carousel_scrolled', --Android: review engagement 
      'listing_all_reviews_screen', --Android: review engagement 
      'reviews_sort_button_clicked', --Android: review engagement 
      'review_details_bottom_sheet', --Android: review engagement 
      'highlighted_review_clicked', --Android: review engagement 
      'fullscreen_review_media_screen', --Android: review engagement 
      'listing_media_gallery_scrolled',--Android: review engagement 
      'listing_page_image_carousel_changed'--Android: review engagement 
    ) then coalesce(event_value,filtered_event_value) else null end) as review_engagement_actions,
  sum(case 
      when event_id in (
      'view_listing', -- all platforms: listing view
      'listing_expand_description_open', -- web: open description 
      'product_details_content_toggle_open',-- web: open description 
      'shop_home', -- all platforms: shop home
      'cart_view', -- all platforms: cart
      'search', -- all platforms: search
      'appreciation_photo_carousel_thumbnails_pressed_next_listing_page', -- web: image scrolling
      'image_carousel_swipe', -- web: image scrolling
      'listing_page_image_carousel_changed', -- boe: image scrolling
      'listing_page_review_engagement_frontend', -- web: listing engagement 
      'listing_see_all_reviews_tapped', --iOS: review engagement 
      'listing_screen_review_card_swipe', --iOS: review engagement 
      'review_card_tapped',--iOS: review engagement 
      'review_updates_view_shop_home_reviews', --iOS: review engagement 
      'listing_screen_reviews_seen',--iOS: review engagement 
      'fullscreen_review_media_screen', --iOS: review engagement 
      'reviews_sort_suggested_clicked', --iOS: review engagement 
      'reviews_sort_most_recent_clicked', --iOS: review engagement 
      'reviews_sort_highest_rated_clicked', --iOS: review engagement 
      'reviews_sort_lowest_rated_clicked', --iOS: review engagement  
      'listing_image_swipe',--iOS: review engagement 
      'see_all_reviews_clicked', --Android: review engagement 
      'listing_reviews_carousel_scrolled', --Android: review engagement 
      'listing_all_reviews_screen', --Android: review engagement 
      'reviews_sort_button_clicked', --Android: review engagement 
      'review_details_bottom_sheet', --Android: review engagement 
      'highlighted_review_clicked', --Android: review engagement 
      'fullscreen_review_media_screen', --Android: review engagement 
      'listing_media_gallery_scrolled',--Android: review engagement 
      'listing_page_image_carousel_changed',--Android: review engagement 
      'listing_item_details_read_description_clicked',-- Android: description expanded
      'listing_item_details_read_more_description_tapped' -- iOS: description expanded 
    ) then coalesce(event_value,filtered_event_value) else null end) as total_trust_building_actions,
/* FUNNEL PROGRESSION AGGS */
  sum(case when event_id in ('backend_add_to_cart') then coalesce(event_value,filtered_event_value) else null end) as atc_actions,
  sum(case when event_id in ('backend_favorite_item2') then coalesce(event_value,filtered_event_value) else null end) as favorting_actions,  
  sum(case when event_id in ('checkout_start') then coalesce(event_value,filtered_event_value) else null end) as checkout_start_actions,  
  sum(case when event_id in ('backend_cart_payment') then coalesce(event_value,filtered_event_value) else null end) as conversion_actions,  
  sum(case 
    when event_id in (
      'backend_add_to_cart', -- all platforms: add to cart
      'backend_favorite_item2', -- all platforms: favorite
      'checkout_start', -- all platforms: checkout start
      'backend_cart_payment'-- all platforms: conversion rate 
    ) then coalesce(event_value,filtered_event_value) else null end) as total_funnel_progression,
  sum(case when event_id in ('backend_send_convo') then coalesce(event_value,filtered_event_value) else null end) as convos_sent_count, 
  count(distinct bucketing_id) as bucketed_count
from  
  etsy-data-warehouse-prod.catapult_unified.aggregated_event_daily a
inner join 
  experiments c 
    on a.experiment_id = c.config_flag
    and a._date between start_date and end_date -- dates experiment was live
where 1=1
  and event_id in 
    ( /* TRUST BUILDING */
      'view_listing', -- all platforms: listing view
      'listing_expand_description_open', -- web: open description 
      'product_details_content_toggle_open',-- web: open description 
      'shop_home', -- all platforms: shop home
      'cart_view', -- all platforms: cart
      'search', -- all platforms: search
      'appreciation_photo_carousel_thumbnails_pressed_next_listing_page', -- web: image scrolling
      'image_carousel_swipe', -- web: image scrolling
      'listing_page_image_carousel_changed', -- boe: image scrolling
      'listing_page_review_engagement_frontend', -- web: listing engagement 
      'listing_see_all_reviews_tapped', --iOS: review engagement 
      'listing_screen_review_card_swipe', --iOS: review engagement 
      'review_card_tapped',--iOS: review engagement 
      'review_updates_view_shop_home_reviews', --iOS: review engagement 
      'listing_screen_reviews_seen',--iOS: review engagement 
      'fullscreen_review_media_screen', --iOS: review engagement 
      'reviews_sort_suggested_clicked', --iOS: review engagement 
      'reviews_sort_most_recent_clicked', --iOS: review engagement 
      'reviews_sort_highest_rated_clicked', --iOS: review engagement 
      'reviews_sort_lowest_rated_clicked', --iOS: review engagement  
      'listing_image_swipe',--iOS: review engagement 
      'see_all_reviews_clicked', --Android: review engagement 
      'listing_reviews_carousel_scrolled', --Android: review engagement 
      'listing_all_reviews_screen', --Android: review engagement 
      'reviews_sort_button_clicked', --Android: review engagement 
      'review_details_bottom_sheet', --Android: review engagement 
      'highlighted_review_clicked', --Android: review engagement 
      'fullscreen_review_media_screen', --Android: review engagement 
      'listing_media_gallery_scrolled',--Android: review engagement 
      'listing_page_image_carousel_changed',--Android: review engagement 
      'listing_item_details_read_description_clicked',-- Android: description expanded
      'listing_item_details_read_more_description_tapped', -- iOS: description expanded 
  /* FUNNEL PROGRESSION */
      'backend_add_to_cart', -- all platforms: add to cart
      'backend_favorite_item2', -- all platforms: favorite
      'checkout_start', -- all platforms: checkout start
      'backend_cart_payment',-- all platforms: conversion rate 
  /* OTHER*/
      'backend_send_convo' -- convos
    )
group by all 
);

-- create or replace table etsy-data-warehouse-dev.rollups.trust_progression_index_experiments as (
select
  coalesce(k.launch_id,t.launch_id) as launch_id,
  coalesce(k.end_date,t.end_date) as end_date,
  coalesce(k.config_flag,t.config_flag) as config_flag,
  coalesce(k.status,t.status) as status,
  coalesce(k.ramp_decision,t.ramp_decision) as ramp_decision,
  coalesce(k.platform,t.platform) as platform,
  coalesce(k.subteam,t.subteam) as subteam,
  coalesce(k.group_name,t.group_name) as group_name,
  coalesce(k.initiative,t.initiative) as initiative,
  variant_rnk,
  variant_id,
  coalesce(k.gms_coverage,t.gms_coverage) as gms_coverage,
  coalesce(k.traffic_coverage,t.traffic_coverage) as traffic_coverage,
  coalesce(k.ads_cr_control,t.ads_cr_control) as ads_cr_control,
  ads_cr_treatment,
  ads_cr_change,
  ads_cr_pvalue,
  ads_cr_sig,
  coalesce(k.gpu_control,t.gpu_control) as gpu_control,
  gpu_treatment,
  gpu_change,
  gpu_pvalue,
  gpu_sig,
  coalesce(k.cr_control,t.cr_control) as cr_control,
  cr_treatment,
  cr_change,
  cr_pvalue,
  cr_sig,
  view_listing_actions,
  open_description_count,
  image_scrolling_count,
  shop_home_count,
  cart_view_count,
  search_count,
  review_engagement_actions,
  total_trust_building_actions,
  atc_actions,
  favorting_actions,  
  checkout_start_actions,  
  conversion_actions,  
  total_funnel_progression,
  total_trust_building_actions/total_funnel_progression as tpi,
  convos_sent_count,
from trust_measurements t
left join key_metrics k 
  on k.launch_id=t.launch_id
  and (k.metric_variant_name=t.variant_id OR t.variant_id is null) -- allows me to join even on 'off'
order by end_date,variant_id asc 
; 
END
