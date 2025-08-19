BEGIN
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
  metric_variant_name,
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
  gms_coverage,
  traffic_coverage,
  ads_cr_control,
  ads_cr_treatment,
  ads_cr_change,
  ads_cr_pvalue,
  gpu_control,
  gpu_treatment,
  gpu_change,
  gpu_pvalue,
  cr_control,
  cr_treatment,
  cr_change,
  cr_pvalue,
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
  launch_id,
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
-- , variants as (
-- select
--   *,
--   concat("variant - ", abs(ranked1 - total_variants)) as variant_id --- cleaning up variants for google sheet
-- from (
-- select
--   experiment_id,
--   variant_id,
--   platform,
--   count(case when variant_id != 'off' then variant_id else null end) over (partition by experiment_id) as total_variants,
--   case when variant_id != 'off' then rank() over (partition by experiment_id order by variant_id desc) else null end as ranked1,
-- from `etsy-data-warehouse-dev.madelinecollins.web_trust_experiments_events_q2` 
-- group by all)
-- )
select 
  launch_id,
  variant_id,
  experiment_id,
  total_trust_building_actions,
  total_funnel_progression,
  total_trust_building_actions/total_funnel_progression as tpi,
  convos_sent_count
from (
  select
    launch_id,
    experiment_id,
    variant_id,
    platform,
    count(case when variant_id not in ('off', 'control') then variant_id else null end) over (partition by experiment_id) as total_variants,
    coalesce(case when variant_id not in ('off', 'control') then rank() over (partition by experiment_id order by variant_id desc) else null end,0) as ranked1,
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
        'backend_cart_payment'
      ) then total_events else null end) as total_funnel_progression,    
    sum(case when event_id in ('backend_send_convo') then total_events else null end) as convos_sent_count,
  from 
   events
  group by all
  )
group by all
);

select
  k.launch_id,
  end_date, 
  config_flag, 
  status,
  ramp_decision,
  k.platform,
  subteam,
  group_name,
  initiative,
  variant_id,
  gms_coverage,
  traffic_coverage,
  ads_cr_control,
  ads_cr_treatment,
  ads_cr_change,
  ads_cr_pvalue,
  gpu_control,
  gpu_treatment,
  gpu_change,
  gpu_pvalue,
  cr_control,
  cr_treatment,
  cr_change,
  cr_pvalue,
  total_trust_building_actions,
  total_funnel_progression,
  tpi,
  convos_sent_count,
  convos_sent_count,
from key_metrics k
inner join trust_measurements t 
  on k.launch_id=t.launch_id
  and k.metric_variant_name=t.variant_id
order by end_date,variant_id asc 
; 
END


-- COMPARING TRUST METRICS
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
  launch_id,
  end_date, 
  a.experiment_id,
  variant_id,
  ramp_decision,
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
-- , variants as (
-- select
--   *,
--   concat("variant - ", abs(ranked1 - total_variants)) as variant_id --- cleaning up variants for google sheet
-- from (
-- select
--   experiment_id,
--   variant_id,
--   platform,
--   count(case when variant_id != 'off' then variant_id else null end) over (partition by experiment_id) as total_variants,
--   case when variant_id != 'off' then rank() over (partition by experiment_id order by variant_id desc) else null end as ranked1,
-- from `etsy-data-warehouse-dev.madelinecollins.web_trust_experiments_events_q2` 
-- group by all)
-- )
select 
  platform,
  end_date,
  experiment_id,
  launch_id,
  variant_id,
  ramp_decision,
  total_trust_building_actions,
  total_funnel_progression,
  total_trust_building_actions/total_funnel_progression as tpi,
  -- convos_sent_count
from (
  select
    launch_id,
    experiment_id,
    variant_id,
    platform,
    ramp_decision,
    end_date,
    count(case when variant_id not in ('off', 'control') then variant_id else null end) over (partition by experiment_id) as total_variants,
    coalesce(case when variant_id not in ('off', 'control') then rank() over (partition by experiment_id order by variant_id desc) else null end,0) as ranked1,
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
        'backend_cart_payment'
      ) then total_events else null end) as total_funnel_progression,    
    sum(case when event_id in ('backend_send_convo') then total_events else null end) as convos_sent_count,
  from 
   events
  group by all
  )
group by all
order by platform, experiment_id, variant_id desc
