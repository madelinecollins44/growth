------------------------- PULL ALL KEY METRICS
with coverages as (
select 
  launch_id,
  max(case when coverage_name in ('GMS coverage') then coverage_value end) as gms_coverage,
  max(case when coverage_name in ('Traffic coverage') then coverage_value end) as traffic_coverage,
from 
  etsy-data-warehouse-prod.catapult.results_coverage_day rcd
where 1=1
  and coverage_name in ('GMS coverage','Traffic coverage')
  and unit in ('PERCENTAGE')
  and lower(segmentation) in ('any')
  and lower(segment) in('all')
  and launch_id in (1371917948360, 1371917262814)
group by all
)
, metrics as (
select
  launch_id,
  boundary_start_sec,
  metric_variant_name,
  coalesce(dense_rank() over(partition by launch_id, boundary_start_sec order by metric_variant_name asc),0)as variant_rnk,

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
where 1=1
  and segmentation in ('any')
  and segment in ('all')
  and metric_id in (
      1029227163677, -- CR
      1275588643427, -- GMS per Unit
      1227229423992 -- Ads CR
  )
  and launch_id in (1371917948360, 1371917262814)
group by all )
select
  mtcs.launch_id,
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
  metrics mtcs
inner join 
  coverages cvg
    on cvg.launch_id=mtcs.launch_id
order by metric_variant_name asc

------------------------- PULL ALL TRUST METRICS 
select
  experiment_id,
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
where 1=1
  and lower(experiment_id) in ('local_pe.q2_2025.buyer_trust_accelerator.user','local_pe.q2_2025.buyer_trust_accelerator.browser')
  and a._date between '2025-04-22' and '2025-07-29' -- dates experiment was live
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
