BEGIN
  
-- PULL EXPERIMENT BACKGROUND INFO 
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
  and end_date >= '2025-03-01'
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

-- PULL TRUST INDICATOR METRICS
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
  sum(case 
      when event_id in (
      'view_listing',
      'listing_expand_description_open',
      'product_details_content_toggle_open',
      'shop_home',
      'cart_view',
      'search',
      'appreciation_photo_carousel_thumbnails_pressed_next_listing_page',
      'image_carousel_swipe',
      'listing_page_review_engagement_frontend'
    )
  then coalesce(event_value,filtered_event_value) else null end) as total_trust_building_actions,
  sum(case 
    when event_id in (
      'add_to_cart',
      'backend_favorite_item2',
      'checkout_start',
      'backend_cart_payment'
    )
  then coalesce(event_value,filtered_event_value) else null end) as total_funnel_progression,
   sum(case when event_id in ('backend_send_convo') then coalesce(event_value,filtered_event_value) else null end) as convos_sent_count,
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
);

select
  k.launch_id,
  k.end_date, 
  k.config_flag, 
  k.status,
  k.ramp_decision,
  k.platform,
  k.subteam,
  group_name,
  k.initiative,
  k.variant_id,
  gms_coverage,
  traffic_coverage,
  ads_cr_control,
  ads_cr_treatment,
  ads_cr_change,
  ads_cr_pvalue,
  ads_cr_sig,
  gpu_control,
  gpu_treatment,
  gpu_change,
  gpu_pvalue,
  gpu_sig,
  cr_control,
  cr_treatment,
  cr_change,
  cr_pvalue,
  cr_sig,
  total_trust_building_actions,
  total_funnel_progression,
  tpi,
  convos_sent_count,
from key_metrics k
inner join trust_measurements t 
  on k.launch_id=t.launch_id
  and k.metric_variant_name=t.variant_id
order by end_date,variant_id asc 
; 
END
  
---------------------------------------------------------------------------------
--  COMPARING TRUST METRICS INDIVUALLY, WITHOUT KHM 
---------------------------------------------------------------------------------
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
   sum(case 
      when event_id in (
      'view_listing',
      'listing_expand_description_open',
      'product_details_content_toggle_open',
      'shop_home',
      'cart_view',
      'search',
      'appreciation_photo_carousel_thumbnails_pressed_next_listing_page',
      'image_carousel_swipe',
      'listing_page_review_engagement_frontend'
    )
  then coalesce(event_value,filtered_event_value) else null end) as total_trust_building_actions,
  sum(case 
    when event_id in (
      'add_to_cart',
      'backend_favorite_item2',
      'checkout_start',
      'backend_cart_payment'
    )
  then coalesce(event_value,filtered_event_value) else null end) as total_funnel_progression,
  --  sum(case when event_id in ('backend_send_convo') then coalesce(event_value,filtered_event_value) else null end) as convos_sent_count,
  -- count(distinct bucketing_id) as count
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

---------------------------------------------------------------------------------
  -- ELEMENT BREAKDOWN
---------------------------------------------------------------------------------
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
