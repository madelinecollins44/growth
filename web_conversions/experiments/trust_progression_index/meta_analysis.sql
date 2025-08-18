---------------------------------------------------------------------------------
-- PULL EXPERIMENT BACKGROUND INFO 
---------------------------------------------------------------------------------
create or replace temp table key_metrics as (
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
, coverages as (
select 
  e.launch_id,
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
  e.*,
  metric_display_name,
  metric_value_control,
  metric_value_treatment,
  relative_change,
  p_value
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
  )); 


---------------------------------------------------------------------------------
-- PULL TRUST INDICATOR METRICS
---------------------------------------------------------------------------------
create or replace temp table trust_measurements as (
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
, events as (
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
group by all
)
, variants as (
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
from `etsy-data-warehouse-dev.madelinecollins.web_trust_experiments_events_q2` 
group by all)
)
select 
  *,
  case 
    when variant_id in ('off','control') then 'off' 
    when variant_id not in ('off', 'control') then concat("variant - ", case when ranked1 = 1 then 1 when ranked1 = 2 then 2 else ranked1 end) else null end as variant_id_clean, --- cleaning up variants for google sheet
  total_trust_building_actions/total_funnel_progression as tpi
from (
  select
    experiment_id,
    variant_id,
    platform,
    count(case when variant_id not in ('off', 'control') then variant_id else null end) over (partition by experiment_id) as total_variants,
    case when variant_id not in ('off', 'control') then rank() over (partition by experiment_id order by variant_id desc) else null end as ranked1,
    sum(case when event_id in (
        'view_listing',  --view listing
        'listing_expand_description_open','product_details_content_toggle_open' --- open description
        'shop_home', --- shop home
        'cart_view', -- cart view
        'search', --search
        'listing_page_image_carousel_changed','image_carousel_swipe' ---image scrolling
        'listing_page_review_engagement_frontend' -- engagement
      ) then total_events else null end) as total_trust_building_actions,
    sum(case when event_id in ('add_to_cart',
          'backend_favorite_item2',
          'checkout_start',
          'backend_cart_payment') then total_events else null end) as total_funnel_progression
  from 
    `etsy-data-warehouse-dev.madelinecollins.web_trust_experiments_events_q2`
  group by all)
);
