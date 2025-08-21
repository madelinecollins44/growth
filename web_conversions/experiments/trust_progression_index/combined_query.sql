BEGIN
---- PULL ALL KEY METRICS
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
