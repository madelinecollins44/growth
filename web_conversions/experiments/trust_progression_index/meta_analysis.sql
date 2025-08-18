---------------------------------------------------------------------------------
-- PULL EXPERIMENT BACKGROUND INFO 
---------------------------------------------------------------------------------
with experiments as (
select
  launch_id,
  end_date, 
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
  and group_name in ('Drive Conversion')
  and subteam in ('RegX')
  and end_date >= '2025-04-01'
  and platform in ('mobile_web','desktop')
)
, coverages as (
select 
  launch_id,
  coverage_name,
  coverage_value,
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
)
select
  metric_display_name,
  metric_value_control,
  metric_value_treatment,
  relative_change,
  p_value
from 
  etsy-data-warehouse-prod.catapult.results_metric_day rmd
inner join 
  experiments e
   on rms.launch_id=e.launch_id
   and rmd._date=e.end_date
where 1=1
  and segmentation in ('any')
  and segment in ('all')
  and metric_id in (
      '1029227163677', -- CR
      '1275588643427', -- GMS per Unit
      '1227229423992', -- Ads CR
  )

---------------------------------------------------------------------------------
-- PULL TRUST INDICATOR METRICS
---------------------------------------------------------------------------------
create or replace table `etsy-data-warehouse-dev.madelinecollins.boe_trust_experiments_events_q2` as (
with experiments as (
select 
  launch_id,
  end_date, 
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
  and end_date >= '2025-04-01'
  and subteam in ('RegX')
)
select 
  a.experiment_id,
  variant_id,
  platform,
  a.event_id,
  count(*) as counts, 
  sum(event_value) as total_events
from 
  etsy-data-warehouse-prod.catapult_unified.aggregated_event_daily a
inner join experiments c on a.experiment_id = c.config_flag
where a._date >= '2025-04-01'
and event_id in 
  ( /* TRUST BUILDING */
    'view_listing',  --view listing
  'product_details_content_toggle_open' --- open description
  'shop_home', --- shop home
  'cart_view', -- cart view
  'search', --search
  'listing_page_image_carousel_changed','image_carousel_swipe' ---image scrolling
  'listing_page_review_engagement_frontend', -- engagement
 /* FUNNEL PROGRESSION */
  'backend_favorite_item2', -- favorited
  'add_to_cart', --A2C
  'checkout_start', --- checkout start
  'backend_cart_payment', --- conversion rate
  'backend_send_convo' -- convo
  )
group by all);
