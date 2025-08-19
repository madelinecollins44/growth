create or replace table `etsy-data-warehouse-dev.madelinecollins.boe_trust_experiments_events_q2` as
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
  experiment_id,
  start_date,
  end_date,
  variant_id,
  platform,
  event_id,
  case when filter_flag is null then total_events else filtered_events end as total_events,
  case when filter_flag is null then unique_count else unique_filtered_count end as total_uniques
from (
select 
  a.experiment_id,
  start_date,
  end_date,
  variant_id,
  platform,
   case 
    when event_id in (
      'view_listing',  --view listing
      'listing_expand_description_open','product_details_content_toggle_open' --- open description
      'shop_home', --- shop home
      'cart_view', -- cart view
      'search', --search
      'appreciation_photo_carousel_thumbnails_pressed_next_listing_page','image_carousel_swipe' ---image scrolling
      'listing_page_review_engagement_frontend' -- engagement
      ) then 'trust_building_actions'
     when event_id in (
        'add_to_cart',
        'backend_favorite_item2',
        'checkout_start',
        'backend_cart_payment'
      ) then 'funnel_progression'
      else 'none'
    end as tpi_element,
  a.event_id,
  min(filtered_bucketing_ts) as filter_flag,
  sum(filtered_event_value) as filtered_events,
  count(distinct bucketing_id) as unique_count, 
  count(distinct case when filtered_bucketing_ts is not null then bucketing_id else null end) as unique_filtered_count,
  sum(event_value) as total_events
from 
  etsy-data-warehouse-prod.catapult_unified.aggregated_event_daily a
inner join experiments c on a.experiment_id = c.config_flag
and a._date between start_date and end_date
where a._date >= '2025-03-01'
and event_id in 
  ( /* TRUST BUILDING */
    'view_listing',  --view listing
  'product_details_content_toggle_open' --- open description
  'shop_home', --- shop home
  'cart_view', -- cart view
  'search', --search
  'appreciation_photo_carousel_thumbnails_pressed_next_listing_page','image_carousel_swipe' ---image scrolling
  'listing_page_review_engagement_frontend', -- engagement
 /* FUNNEL PROGRESSION */
  'backend_favorite_item2', -- favorited
  'add_to_cart', --A2C
  'checkout_start', --- checkout start
  'backend_cart_payment', --- conversion rate
  'backend_send_convo' -- convo
  )
group by all);

select
  *,
  concat("variant - ", abs(ranked1 - total_variants)) as variant_id --- cleaning up variants for google sheet
from (
select
  experiment_id,
  variant_id,
  platform,
  count(case when variant_id != 'off' then variant_id else null end) over (partition by experiment_id) as total_variants,
  case when variant_id != 'off' then rank() over (partition by experiment_id order by variant_id desc) else null end as ranked1,
from `etsy-data-warehouse-dev.madelinecollins.boe_trust_experiments_events` 
group by all);
