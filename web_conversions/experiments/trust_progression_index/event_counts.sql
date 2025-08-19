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
  initiative
from 
  etsy-data-warehouse-prod.rollups.experiment_reports 
where 1=1
  and (trim(lower(initiative)) like '%drive conversion%')
  and end_date >= '2025-03-01'
  and subteam in ('RegX')
)
select
  launch_id,
  end_date, 
  config_flag, 
  ramp_decision,
  platform,
  variant_id,
  event_id,
  sum(case 
    when bucketing_ts is not null then event_value 
    when filtered_bucketing_ts is not null then filtered_event_value 
  end) as event_value_case,
  sum(coalesce(event_value,filtered_event_value)) as event_value_coalesce,
  count(distinct bucketing_id) as count
from  
  etsy-data-warehouse-prod.catapult_unified.aggregated_event_daily a
inner join 
  experiments c 
    on a.experiment_id = c.config_flag
    and a._date between start_date and end_date -- dates experiment was live
where 1=1
  and event_id in 
    ( /* TRUST BUILDING */
      'view_listing',  --view listing
    'product_details_content_toggle_open' --- open description
    'shop_home', --- shop home
    'cart_view', -- cart view
    'search', --search
    'appreciation_photo_carousel_thumbnails_pressed_next_listing_page','image_carousel_swipe', ---image scrolling
    'listing_page_review_engagement_frontend', -- engagement
  /* FUNNEL PROGRESSION */
    'backend_favorite_item2', -- favorited
    'backend_add_to_cart', --A2C
    'checkout_start', --- checkout start
    'backend_cart_payment', --- conversion rate
    'backend_send_convo' -- convo
    )
    -- and experiment_id in ('growth_regx.lp_bb_tenure_mweb')
group by all 
