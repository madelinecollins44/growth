create or replace table etsy-data-warehouse-dev.rollups.custom_drive_conversion_regx_experiment_reporting as (
with plats as (
-- This CTE gets the platform for each experiment (launch_id)
select 
  distinct launch_id
  , update_date
  , name as platform
  , dense_rank() over(partition by launch_id  order by update_date desc) AS row_num
from 
  `etsy-data-warehouse-prod.etsy_atlas.catapult_launches_expected_platforms`
qualify 
  row_num = 1
)
, plats_agg as (
select 
  launch_id
  , string_agg(platform order by platform) as platform
from plats
group by all
)

, exp_coverage as (
-- This CTE gets the coverage %'s for each experiment. It should match up with what's shown in the catapult page
select
  launch_id
  , coverage_name
  , date(timestamp_seconds(boundary_start_sec)) as start_date
  , date(timestamp_seconds(boundary_end_sec)) as end_date
  , dense_rank() over (partition by launch_id, date(timestamp_seconds(boundary_start_sec)) order by _date desc) as date_rank
  , cast(coverage_value/100 as float64) as coverage_value
from `etsy-data-warehouse-prod.catapult.results_coverage_day`
where segmentation = "any"
  and segment = "all"
qualify
  date_rank=1
)
, exp_coverage_agg as (
select
  launch_id
  , start_date
  , end_date
  , max(case when coverage_name = 'GMS coverage' then coverage_value else null end) as gms_coverage
  , max(case when coverage_name = 'Traffic coverage' then coverage_value else null end) as traffic_coverage
  , max(case when coverage_name = 'Offsite Ads coverage' then coverage_value else null end) as osa_coverage
  , max(case when coverage_name = 'Prolist coverage' then coverage_value else null end) as prolist_coverage
from exp_coverage
group by  
  all
), exp_metrics as (
-- This CTE gathers all the metric ids and the corresponding names included in the experiment, along with whether or not a metric is the success metric.
select
  cem.launch_id
  , cem.metric_id
  , cm.name
  , cem.is_success_criteria
from `etsy-data-warehouse-prod.etsy_atlas.catapult_experiment_metrics`  cem
left join `etsy-data-warehouse-prod.etsy_atlas.catapult_metrics` cm
  on cem.metric_id = cm.metric_id
group by all -- has duplicate rows
)
, metrics_list as (
-- This CTE grabs all of the metric values from the experiment.
-- Since the results_metric_day table contains values for each day of the experiment (and multiple boundaries if relevant), the date_rnk is used to get the last date of the experiment.
-- There can be multiple values for a metric on the final day (usually if there is a metric that also has a "cuped" value), the metric_rnk is used to grab the metric that has been "cuped" if there
-- are multiple values by choosing the one with the longest metric_stat_methodology. 
select  
  launch_id
  , _date
  , boundary_start_sec as start_date
  , boundary_end_sec as end_date
  , metric_variant_name
  , metric_display_name
  , metric_id
  , metric_value_control
  , metric_value_treatment
  , relative_change
  , p_value
  , dense_rank() over(partition by launch_id, boundary_start_sec order by metric_variant_name asc) as variant_rnk
  , dense_rank() over(partition by launch_id, boundary_start_sec order by _date desc) as date_rnk
  , row_number() over(partition by launch_id, metric_variant_name, boundary_start_sec,_date ,lower(metric_display_name) order by length(metric_stat_methodology) desc) as metric_rnk
from `etsy-data-warehouse-prod.catapult.results_metric_day` 
where 
  1=1
  and segmentation = "any"
  and segment = "all"
qualify
  date_rnk=1 and metric_rnk=1
)
, metrics_agg as (
-- This CTE aggregates all of the relevant metrics. Here is where we can add in new metrics if needed. 
select
  ml.launch_id
  , ml.start_date
  , ml.end_date
  , max(case when ml.variant_rnk=1 then metric_variant_name else null end) as variant1_name
  , max(case when ml.variant_rnk=2 then metric_variant_name else null end) as variant2_name
  -- target metric
  , max(case when em.is_success_criteria>0 and ml.variant_rnk=1 then em.name else null end) as target_metric
  , max(case when em.is_success_criteria>0 and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_value_target_metric
  , max(case when em.is_success_criteria>0 and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_value_target_metric
  , max(case when em.is_success_criteria>0 and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_target_metric
  , max(case when em.is_success_criteria>0 and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_target_metric
  -- Conversion rate
  , max(case when lower(ml.metric_display_name) = 'conversion rate' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_conversion_rate
  , max(case when lower(ml.metric_display_name) = 'conversion rate' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_conversion_rate
  , max(case when lower(ml.metric_display_name) = 'conversion rate' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_conversion_rate
  , max(case when lower(ml.metric_display_name) = 'conversion rate' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_conversion_rate
  -- Percent with add to cart
  , max(case when lower(ml.metric_display_name) = 'percent with add to cart' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_pct_atc
  , max(case when lower(ml.metric_display_name) = 'percent with add to cart' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_pct_atc
  , max(case when lower(ml.metric_display_name) = 'percent with add to cart' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_pct_atc
  , max(case when lower(ml.metric_display_name) = 'percent with add to cart' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_pct_atc
  -- Listing view
  , max(case when lower(ml.metric_display_name) = 'percent with listing view' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_pct_listing_view
  , max(case when lower(ml.metric_display_name) = 'percent with listing view' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_pct_listing_view
  , max(case when lower(ml.metric_display_name) = 'percent with listing view' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_pct_listing_view
  , max(case when lower(ml.metric_display_name) = 'percent with listing view' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_pct_listing_view
  -- Shop home 
  , max(case when lower(ml.metric_display_name) = 'percent with shop home view' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_pct_w_shop_home_view
  , max(case when lower(ml.metric_display_name) = 'percent with shop home view' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_pct_w_shop_home_view
  , max(case when lower(ml.metric_display_name) = 'percent with shop home view' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_pct_w_shop_home_view
  , max(case when lower(ml.metric_display_name) = 'percent with shop home view' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_pct_w_shop_home_view
  -- Mean visit
  , max(case when lower(ml.metric_display_name) = 'mean visits' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_mean_visits
  , max(case when lower(ml.metric_display_name) = 'mean visits' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_mean_visits
  , max(case when lower(ml.metric_display_name) = 'mean visits' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_mean_visits
  , max(case when lower(ml.metric_display_name) = 'mean visits' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_mean_visits
  -- GMS per unit
  , max(case when lower(ml.metric_display_name) = 'gms per unit' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_gms_per_unit
  , max(case when lower(ml.metric_display_name) = 'gms per unit' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_gms_per_unit
  , max(case when lower(ml.metric_display_name) = 'gms per unit' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_gms_per_unit
  , max(case when lower(ml.metric_display_name) = 'gms per unit' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_gms_per_unit
  -- Mean engaged visit
  , max(case when lower(ml.metric_display_name) = 'mean engaged_visit' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_mean_engaged_visit
  , max(case when lower(ml.metric_display_name) = 'mean engaged_visit' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_mean_engaged_visit
  , max(case when lower(ml.metric_display_name) = 'mean engaged_visit' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_mean_engaged_visit
  , max(case when lower(ml.metric_display_name) = 'mean engaged_visit' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_mean_engaged_visit
  -- ADs Conversion rate
  , max(case when lower(ml.metric_display_name) = 'ads conversion rate' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_ads_cvr
  , max(case when lower(ml.metric_display_name) = 'ads conversion rate' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_ads_cvr
  , max(case when lower(ml.metric_display_name) = 'ads conversion rate' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_ads_cvr
  , max(case when lower(ml.metric_display_name) = 'ads conversion rate' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_ads_cvr
  -- ADs ACxV
  , max(case when lower(ml.metric_display_name) in ('ads winsorized ac*v ($100)','ads winsorized acvv ($100)') and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_ads_acxv
  , max(case when lower(ml.metric_display_name) in ('ads winsorized ac*v ($100)','ads winsorized acvv ($100)') and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_ads_acxv
  , max(case when lower(ml.metric_display_name) in ('ads winsorized ac*v ($100)','ads winsorized acvv ($100)') and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_ads_acxv
  , max(case when lower(ml.metric_display_name) in ('ads winsorized ac*v ($100)','ads winsorized acvv ($100)') and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_ads_acxv
  -- Winsorized ACxV
  , max(case when lower(ml.metric_display_name) = 'winsorized ac*v' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_winsorized_acxv
  , max(case when lower(ml.metric_display_name) = 'winsorized ac*v' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_winsorized_acxv
  , max(case when lower(ml.metric_display_name) = 'winsorized ac*v' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_winsorized_acxv
  , max(case when lower(ml.metric_display_name) = 'winsorized ac*v' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_winsorized_acxv
  -- OCB
  , max(case when lower(ml.metric_display_name) = 'orders per converting browser (ocb)' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_ocb
  , max(case when lower(ml.metric_display_name) = 'orders per converting browser (ocb)' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_ocb
  , max(case when lower(ml.metric_display_name) = 'orders per converting browser (ocb)' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_ocb
  , max(case when lower(ml.metric_display_name) = 'orders per converting browser (ocb)' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_ocb
  -- Orders per unit
  , max(case when lower(ml.metric_display_name) = 'orders per unit' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_opu
  , max(case when lower(ml.metric_display_name) = 'orders per unit' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_opu
  , max(case when lower(ml.metric_display_name) = 'orders per unit' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_opu
  , max(case when lower(ml.metric_display_name) = 'orders per unit' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_opu
  -- Winsorized AOV
  , max(case when lower(ml.metric_display_name) = 'winsorized aov' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_aov
  , max(case when lower(ml.metric_display_name) = 'winsorized aov' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_aov
  , max(case when lower(ml.metric_display_name) = 'winsorized aov' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_aov
  , max(case when lower(ml.metric_display_name) = 'winsorized aov' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_aov  
  -- Prolist Spend
  , max(case when lower(ml.metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_mean_prolist_spend
  , max(case when lower(ml.metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_mean_prolist_spend
  , max(case when lower(ml.metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_mean_prolist_spend
  , max(case when lower(ml.metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_mean_prolist_spend
  -- OSA Revenue
  , max(case when lower(ml.metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_mean_osa_revenue
  , max(case when lower(ml.metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_mean_osa_revenue
  , max(case when lower(ml.metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_mean_osa_revenue
  , max(case when lower(ml.metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_mean_osa_revenue
    -- LP Review Engagement 
  , max(case when lower(ml.metric_display_name) = 'percent with listing_page_review_engagement_frontend' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_lp_review_engagement
  , max(case when lower(ml.metric_display_name) = 'percent with listing_page_review_engagement_frontend' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_lp_review_engagement
  , max(case when lower(ml.metric_display_name) = 'percent with listing_page_review_engagement_frontend' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_lp_review_engagement
  , max(case when lower(ml.metric_display_name) = 'percent with listing_page_review_engagement_frontend' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_lp_review_engagement
    -- LP Review Pagination
  , max(case when lower(ml.metric_display_name) = 'percent with listing_page_reviews_pagination' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_lp_review_pagination
  , max(case when lower(ml.metric_display_name) = 'percent with listing_page_reviews_pagination' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_lp_review_pagination
  , max(case when lower(ml.metric_display_name) = 'percent with listing_page_reviews_pagination' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_lp_review_pagination
  , max(case when lower(ml.metric_display_name) = 'percent with listing_page_reviews_pagination' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_lp_review_pagination
      -- LP Review Photo Opened
  , max(case when lower(ml.metric_display_name) = 'percent with appreciation_photo_overlay_opened' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_lp_review_photo_opened
  , max(case when lower(ml.metric_display_name) = 'percent with appreciation_photo_overlay_opened' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_lp_review_photo_opened
  , max(case when lower(ml.metric_display_name) = 'percent with appreciation_photo_overlay_opened' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_lp_review_photo_opened
  , max(case when lower(ml.metric_display_name) = 'percent with appreciation_photo_overlay_opened' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_lp_review_photo_opened
      -- LP Review Sort
  , max(case when lower(ml.metric_display_name) = 'percent with reviews_sort_by_changed' and ml.variant_rnk=1 then ml.metric_value_control else null end) as control_lp_review_sort
  , max(case when lower(ml.metric_display_name) = 'percent with reviews_sort_by_changed' and ml.variant_rnk=1 then ml.metric_value_treatment else null end) as variant1_lp_review_sort
  , max(case when lower(ml.metric_display_name) = 'percent with reviews_sort_by_changed' and ml.variant_rnk=1 then ml.relative_change else null end)/100 as variant1_pct_change_lp_review_sort
  , max(case when lower(ml.metric_display_name) = 'percent with reviews_sort_by_changed' and ml.variant_rnk=1 then ml.p_value else null end) as variant1_pval_lp_review_sort
  -- Variant 2 Conversion rate
  , max(case when lower(ml.metric_display_name) = 'conversion rate' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_conversion_rate
  , max(case when lower(ml.metric_display_name) = 'conversion rate' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_conversion_rate
  , max(case when lower(ml.metric_display_name) = 'conversion rate' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_conversion_rate
    -- Variant 2 Percent with add to cart
  , max(case when lower(ml.metric_display_name) = 'Percent with add to cart' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_pct_atc
  , max(case when lower(ml.metric_display_name) = 'Percent with add to cart' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_pct_atc
  , max(case when lower(ml.metric_display_name) = 'Percent with add to cart' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_pct_atc
  -- Variant 2 Listing view
  , max(case when lower(ml.metric_display_name) = 'Percent with listing view' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_pct_listing_view
  , max(case when lower(ml.metric_display_name) = 'Percent with listing view' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_pct_listing_view
  , max(case when lower(ml.metric_display_name) = 'Percent with listing view' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_pct_listing_view
  -- Variant 2 Shop home 
  , max(case when lower(ml.metric_display_name) = 'Percent with shop home view' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_pct_w_shop_home_view
  , max(case when lower(ml.metric_display_name) = 'Percent with shop home view' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_pct_w_shop_home_view
  , max(case when lower(ml.metric_display_name) = 'Percent with shop home view' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_pct_w_shop_home_view
  -- Variant 2 Mean visits
  , max(case when lower(ml.metric_display_name) = 'mean visits' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_mean_visits
  , max(case when lower(ml.metric_display_name) = 'mean visits' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_mean_visits
  , max(case when lower(ml.metric_display_name) = 'mean visits' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_mean_visits
  -- Variant 2 GMS per unit
  , max(case when lower(ml.metric_display_name) = 'gms per unit' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_gms_per_unit
  , max(case when lower(ml.metric_display_name) = 'gms per unit' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_gms_per_unit
  , max(case when lower(ml.metric_display_name) = 'gms per unit' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_gms_per_unit
  -- Variant 2 Mean engaged visit
  , max(case when lower(ml.metric_display_name) = 'mean engaged_visit' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_mean_engaged_visit
  , max(case when lower(ml.metric_display_name) = 'mean engaged_visit' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_mean_engaged_visit
  , max(case when lower(ml.metric_display_name) = 'mean engaged_visit' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_mean_engaged_visit
  -- Variant 2 ADs Conversion rate
  , max(case when lower(ml.metric_display_name) = 'ads conversion rate' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_ads_cvr
  , max(case when lower(ml.metric_display_name) = 'ads conversion rate' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_ads_cvr
  , max(case when lower(ml.metric_display_name) = 'ads conversion rate' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_ads_cvr
  -- Variant 2 ADs ACxV
  , max(case when lower(ml.metric_display_name) in ('ads winsorized ac*v ($100)','ads winsorized acvv ($100)') and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_ads_acxv
  , max(case when lower(ml.metric_display_name) in ('ads winsorized ac*v ($100)','ads winsorized acvv ($100)') and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_ads_acxv
  , max(case when lower(ml.metric_display_name) in ('ads winsorized ac*v ($100)','ads winsorized acvv ($100)') and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_ads_acxv
  -- Variant 2 Winsorized ACxV
  , max(case when lower(ml.metric_display_name) = 'winsorized ac*v' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_winsorized_acxv
  , max(case when lower(ml.metric_display_name) = 'winsorized ac*v' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_winsorized_acxv
  , max(case when lower(ml.metric_display_name) = 'winsorized ac*v' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_winsorized_acxv
  -- Variant 2 OCB
  , max(case when lower(ml.metric_display_name) = 'orders per converting browser (ocb)' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_ocb
  , max(case when lower(ml.metric_display_name) = 'orders per converting browser (ocb)' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_ocb
  , max(case when lower(ml.metric_display_name) = 'orders per converting browser (ocb)' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_ocb  
  -- Variant 2 Orders per unit
  , max(case when lower(ml.metric_display_name) = 'orders per unit' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_opu
  , max(case when lower(ml.metric_display_name) = 'orders per unit' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_opu
  , max(case when lower(ml.metric_display_name) = 'orders per unit' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_opu  
  -- Variant 2 Winsorized AOV
  , max(case when lower(ml.metric_display_name) = 'winsorized aov' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_aov
  , max(case when lower(ml.metric_display_name) = 'winsorized aov' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_aov
  , max(case when lower(ml.metric_display_name) = 'winsorized aov' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_aov
  -- Variant 2 Prolist Spend
  , max(case when lower(ml.metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_mean_prolist_spend
  , max(case when lower(ml.metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_mean_prolist_spend
  , max(case when lower(ml.metric_display_name) in ('etsy ads click revenue','mean prolist_total_spend') and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_mean_prolist_spend
  -- Variant 2 OSA
  , max(case when lower(ml.metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_mean_osa_revenue
  , max(case when lower(ml.metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_mean_osa_revenue
  , max(case when lower(ml.metric_display_name) in ('offsite ads attributed revenue','mean offsite_ads_one_day_attributed_revenue') and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_mean_osa_revenue
  -- Variant 2 LP Review Engagement 
  , max(case when lower(ml.metric_display_name) = 'percent with listing_page_review_engagement_frontend' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_lp_review_engagement
  , max(case when lower(ml.metric_display_name) = 'percent with listing_page_review_engagement_frontend' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_lp_review_engagement
  , max(case when lower(ml.metric_display_name) = 'percent with listing_page_review_engagement_frontend' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_lp_review_engagement
    -- Variant 2 LP Review Pagination 
  , max(case when lower(ml.metric_display_name) = 'percent with listing_page_reviews_pagination' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_lp_review_pagination
  , max(case when lower(ml.metric_display_name) = 'percent with listing_page_reviews_pagination' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_lp_review_pagination
  , max(case when lower(ml.metric_display_name) = 'percent with listing_page_reviews_pagination' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_lp_review_pagination
      -- Variant 2 LP Review Photo Opened
  , max(case when lower(ml.metric_display_name) = 'percent with appreciation_photo_overlay_opened' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_lp_review_photo_opened
  , max(case when lower(ml.metric_display_name) = 'percent with appreciation_photo_overlay_opened' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_lp_review_photo_opened
  , max(case when lower(ml.metric_display_name) = 'percent with appreciation_photo_overlay_opened' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_lp_review_photo_opened
    -- Variant 2 LP Review Sort
  , max(case when lower(ml.metric_display_name) = 'percent with reviews_sort_by_changed' and ml.variant_rnk=2 then ml.metric_value_treatment else null end) as variant2_lp_review_sort
  , max(case when lower(ml.metric_display_name) = 'percent with reviews_sort_by_changed' and ml.variant_rnk=2 then ml.relative_change else null end)/100 as variant2_pct_change_lp_review_sort
  , max(case when lower(ml.metric_display_name) = 'percent with reviews_sort_by_changed' and ml.variant_rnk=2 then ml.p_value else null end) as variant2_pval_lp_review_sort
from metrics_list ml 
left join exp_metrics em
  on em.launch_id = ml.launch_id 
     and em.metric_id = ml.metric_id
group by 
  all
)
, metrics_agg_clean as (
-- This purpose of this CTE is to make sure that the ramped up variant is in the variant1_.. metric format, if relevant. it is grabbing the ramped variant from the catapult_gms_report table and matching it to the metrics
-- table's variant
select
  cgms.launch_id
  , cgms.status
  , cgms.variant
  , ma.start_date
  , ma.end_date
  , ma.variant1_name
  , ma.variant2_name
  , ma.target_metric
  , ma.control_value_target_metric
  , ma.variant1_value_target_metric
  , ma.variant1_pct_change_target_metric
  , ma.variant1_pval_target_metric
  -- conversion rate
  , ma.control_conversion_rate
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_conversion_rate
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_conversion_rate
      else ma.variant1_conversion_rate
      end as variant1_conversion_rate
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_conversion_rate
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_conversion_rate
      else ma.variant1_pct_change_conversion_rate
      end as variant1_pct_change_conversion_rate 
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_conversion_rate
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_conversion_rate
      else ma.variant1_pval_conversion_rate
      end as variant1_pval_conversion_rate
  -- Percent with add to cart
  , ma.control_pct_atc
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_atc
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_atc
      else ma.variant1_pct_atc
      end as variant1_pct_atc
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_pct_atc
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_pct_atc
      else ma.variant1_pct_change_pct_atc
      end as variant1_pct_change_pct_atc 
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_pct_atc
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_pct_atc
      else ma.variant1_pval_pct_atc
      end as variant1_pval_pct_atc
  -- Listing view
  , ma.control_pct_listing_view
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_listing_view
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_listing_view
      else ma.variant1_pct_listing_view
      end as variant1_pct_listing_view
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_pct_listing_view
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_pct_listing_view
      else ma.variant1_pct_change_pct_listing_view
      end as variant1_pct_change_pct_listing_view 
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_pct_listing_view
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_pct_listing_view
      else ma.variant1_pval_pct_listing_view
      end as variant1_pval_pct_listing_view
  -- Shop home 
  , ma.control_pct_w_shop_home_view
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_w_shop_home_view
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_w_shop_home_view
      else ma.variant1_pct_w_shop_home_view
      end as variant1_pct_w_shop_home_view
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_pct_w_shop_home_view
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_pct_w_shop_home_view
      else ma.variant1_pct_change_pct_w_shop_home_view
      end as variant1_pct_change_pct_w_shop_home_view 
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_pct_w_shop_home_view
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_pct_w_shop_home_view
      else ma.variant1_pval_pct_w_shop_home_view
      end as variant1_pval_pct_w_shop_home_view
  -- mean visits
  , ma.control_mean_visits
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_mean_visits
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_mean_visits
      else ma.variant1_mean_visits
      end as variant1_mean_visits    
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_mean_visits
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_mean_visits
      else ma.variant1_pct_change_mean_visits
      end as variant1_pct_change_mean_visits
  ,case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_mean_visits
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_mean_visits
      else ma.variant1_pval_mean_visits
      end as variant1_pval_mean_visits
  -- gms per unit
  , ma.control_gms_per_unit
  ,case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_gms_per_unit
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_gms_per_unit
      else ma.variant1_gms_per_unit
      end as variant1_gms_per_unit
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_gms_per_unit
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_gms_per_unit
      else ma.variant1_pct_change_gms_per_unit
      end as variant1_pct_change_gms_per_unit
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_gms_per_unit
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_gms_per_unit
      else ma.variant1_pval_gms_per_unit
      end as variant1_pval_gms_per_unit
  -- mean engaged visit
  , ma.control_mean_engaged_visit
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_mean_engaged_visit
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_mean_engaged_visit
      else ma.variant1_mean_engaged_visit
      end as variant1_mean_engaged_visit
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_mean_engaged_visit
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_mean_engaged_visit
      else ma.variant1_pct_change_mean_engaged_visit
      end as variant1_pct_change_mean_engaged_visit
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_mean_engaged_visit
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_mean_engaged_visit
      else ma.variant1_pval_mean_engaged_visit
      end as variant1_pval_mean_engaged_visit
  -- ads conversion rate
  , ma.control_ads_cvr
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_ads_cvr
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_ads_cvr
      else ma.variant1_ads_cvr
      end as variant1_ads_cvr
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_ads_cvr
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_ads_cvr
      else ma.variant1_pct_change_ads_cvr
      end as variant1_pct_change_ads_cvr
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_ads_cvr
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_ads_cvr
      else ma.variant1_pval_ads_cvr
      end as variant1_pval_ads_cvr
  -- ads acxv
  , ma.control_ads_acxv
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_ads_acxv
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_ads_acxv
      else ma.variant1_ads_acxv
      end as variant1_ads_acxv
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_ads_acxv
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_ads_acxv
      else ma.variant1_pct_change_ads_acxv
      end as variant1_pct_change_ads_acxv
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_ads_acxv
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_ads_acxv
      else ma.variant1_pval_ads_acxv
      end as variant1_pval_ads_acxv
  -- winsorized acxv
  , ma.control_winsorized_acxv
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_winsorized_acxv
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_winsorized_acxv
      else ma.variant1_winsorized_acxv
      end as variant1_winsorized_acxv
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_winsorized_acxv
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_winsorized_acxv
      else ma.variant1_pct_change_winsorized_acxv
      end as variant1_pct_change_winsorized_acxv
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_winsorized_acxv
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_winsorized_acxv
      else ma.variant1_pval_winsorized_acxv
      end as variant1_pval_winsorized_acxv
  -- orders per unit
  , ma.control_opu
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_opu
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_opu
      else ma.variant1_opu
      end as variant1_opu
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_opu
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_opu
      else ma.variant1_pct_change_opu
      end as variant1_pct_change_opu
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_opu
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_opu
      else ma.variant1_pval_opu
      end as variant1_pval_opu
  -- aov
  , ma.control_aov
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_aov
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_aov
      else ma.variant1_aov
      end as variant1_aov
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_aov
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_aov
      else ma.variant1_pct_change_aov
      end as variant1_pct_change_aov
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_aov  
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_aov  
      else ma.variant1_pval_aov  
      end as variant1_pval_aov  
  -- mean prolist spend
  , ma.control_mean_prolist_spend
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_mean_prolist_spend
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_mean_prolist_spend
      else ma.variant1_mean_prolist_spend
      end as variant1_mean_prolist_spend
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_mean_prolist_spend
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_mean_prolist_spend
      else ma.variant1_pct_change_mean_prolist_spend
      end as variant1_pct_change_mean_prolist_spend
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_mean_prolist_spend
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_mean_prolist_spend
      else ma.variant1_pval_mean_prolist_spend
      end as variant1_pval_mean_prolist_spend
  -- osa revenue
  , ma.control_mean_osa_revenue
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_mean_osa_revenue
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_mean_osa_revenue
      else ma.variant1_mean_osa_revenue
      end as variant1_mean_osa_revenue
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_mean_osa_revenue
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_mean_osa_revenue
      else ma.variant1_pct_change_mean_osa_revenue
      end as variant1_pct_change_mean_osa_revenue
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_mean_osa_revenue
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_mean_osa_revenue
      else ma.variant1_pval_mean_osa_revenue
      end as variant1_pval_mean_osa_revenue
   -- Listing Page Review Engagement
  , ma.control_lp_review_engagement
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_lp_review_engagement
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_lp_review_engagement
      else ma.variant1_lp_review_engagement
      end as variant1_lp_review_engagement
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_lp_review_engagement
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_lp_review_engagement
      else ma.variant1_pct_change_lp_review_engagement
      end as variant1_pct_change_lp_review_engagement 
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_lp_review_engagement
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_lp_review_engagement
      else ma.variant1_pval_lp_review_engagement
      end as variant1_pval_lp_review_engagement
   -- LP Review Pagination 
  , ma.control_lp_review_pagination
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_lp_review_pagination
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_lp_review_pagination
      else ma.variant1_lp_review_pagination
      end as variant1_lp_review_pagination
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_lp_review_pagination
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_lp_review_pagination
      else ma.variant1_pct_change_lp_review_pagination
      end as variant1_pct_change_lp_review_pagination 
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_lp_review_pagination
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_lp_review_pagination
      else ma.variant1_pval_lp_review_pagination
      end as variant1_pval_lp_review_pagination
   -- LP Review Photo Opened
  , ma.control_lp_review_photo_opened
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_lp_review_photo_opened
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_lp_review_photo_opened
      else ma.variant1_lp_review_photo_opened
      end as variant1_lp_review_photo_opened
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_lp_review_photo_opened
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_lp_review_photo_opened
      else ma.variant1_pct_change_lp_review_photo_opened
      end as variant1_pct_change_lp_review_photo_opened 
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_lp_review_photo_opened
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_lp_review_photo_opened
      else ma.variant1_pval_lp_review_photo_opened
      end as variant1_pval_lp_review_photo_opened
   -- LP Review Sort
  , ma.control_lp_review_sort
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_lp_review_sort
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_lp_review_sort
      else ma.variant1_lp_review_sort
      end as variant1_lp_review_sort
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pct_change_lp_review_sort
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pct_change_lp_review_sort
      else ma.variant1_pct_change_lp_review_sort
      end as variant1_pct_change_lp_review_sort 
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant1_pval_lp_review_sort
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant2_pval_lp_review_sort
      else ma.variant1_pval_lp_review_sort
      end as variant1_pval_lp_review_sort
  -- variant 2 conversion rate
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_conversion_rate
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_conversion_rate
      else ma.variant2_conversion_rate
      end as variant2_conversion_rate
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pct_change_conversion_rate
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pct_change_conversion_rate
      else ma.variant2_pct_change_conversion_rate
      end as variant2_pct_change_conversion_rate
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pval_conversion_rate
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pval_conversion_rate
      else ma.variant2_pval_conversion_rate
      end as variant2_pval_conversion_rate
  -- variant 2 mean visits
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_mean_visits
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_mean_visits
      else ma.variant2_mean_visits
      end as variant2_mean_visits
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pct_change_mean_visits
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pct_change_mean_visits
      else ma.variant2_pct_change_mean_visits
      end as variant2_pct_change_mean_visits
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pval_mean_visits
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pval_mean_visits
      else ma.variant2_pval_mean_visits
      end as variant2_pval_mean_visits
  -- variant 2 gms per unit
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_gms_per_unit
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_gms_per_unit
      else ma.variant2_gms_per_unit
      end as variant2_gms_per_unit
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pct_change_gms_per_unit
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pct_change_gms_per_unit
      else ma.variant2_pct_change_gms_per_unit
      end as variant2_pct_change_gms_per_unit
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pval_gms_per_unit
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pval_gms_per_unit
      else ma.variant2_pval_gms_per_unit
      end as variant2_pval_gms_per_unit
  -- variant 2 mean engaged visit
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_mean_engaged_visit
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_mean_engaged_visit
      else ma.variant2_mean_engaged_visit
      end as variant2_mean_engaged_visit
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pct_change_mean_engaged_visit
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pct_change_mean_engaged_visit
      else ma.variant2_pct_change_mean_engaged_visit
      end as variant2_pct_change_mean_engaged_visit
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pval_mean_engaged_visit
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pval_mean_engaged_visit
      else ma.variant2_pval_mean_engaged_visit
      end as variant2_pval_mean_engaged_visit
  -- variant 2 ads cvr
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_ads_cvr
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_ads_cvr
      else ma.variant2_ads_cvr
      end as variant2_ads_cvr
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pct_change_ads_cvr
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pct_change_ads_cvr
      else ma.variant2_pct_change_ads_cvr
      end as variant2_pct_change_ads_cvr
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pval_ads_cvr
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pval_ads_cvr
      else ma.variant2_pval_ads_cvr
      end as variant2_pval_ads_cvr
  -- variant 2 ads acxv
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_ads_acxv
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_ads_acxv
      else ma.variant2_ads_acxv
      end as variant2_ads_acxv
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pct_change_ads_acxv
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pct_change_ads_acxv
      else ma.variant2_pct_change_ads_acxv
      end as variant2_pct_change_ads_acxv
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pval_ads_acxv
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pval_ads_acxv
      else ma.variant2_pval_ads_acxv
      end as variant2_pval_ads_acxv
  -- variant 2 winsorized acxv
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_winsorized_acxv
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_winsorized_acxv
      else ma.variant2_winsorized_acxv
      end as variant2_winsorized_acxv
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pct_change_winsorized_acxv
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pct_change_winsorized_acxv
      else ma.variant2_pct_change_winsorized_acxv
      end as variant2_pct_change_winsorized_acxv
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pval_winsorized_acxv
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pval_winsorized_acxv
      else ma.variant2_pval_winsorized_acxv
      end as variant2_pval_winsorized_acxv
  -- variant 2 orders per unit
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_opu
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_opu
      else ma.variant2_opu
      end as variant2_opu
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pct_change_opu
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pct_change_opu
      else ma.variant2_pct_change_opu
      end as variant2_pct_change_opu
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pval_opu  
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pval_opu  
      else ma.variant2_pval_opu  
      end as variant2_pval_opu  
  --variant 2 aov
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_aov
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_aov
      else ma.variant2_aov
      end as variant2_aov
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pct_change_aov
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pct_change_aov
      else ma.variant2_pct_change_aov
      end as variant2_pct_change_aov
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pval_aov
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pval_aov
      else ma.variant2_pval_aov
      end as variant2_pval_aov
  -- variant 2 mean prolist spend
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_mean_prolist_spend
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_mean_prolist_spend
      else ma.variant2_mean_prolist_spend
      end as variant2_mean_prolist_spend
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pct_change_mean_prolist_spend
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pct_change_mean_prolist_spend
      else ma.variant2_pct_change_mean_prolist_spend
      end as variant2_pct_change_mean_prolist_spend
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pval_mean_prolist_spend
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pval_mean_prolist_spend
      else ma.variant2_pval_mean_prolist_spend
      end as variant2_pval_mean_prolist_spend
  -- variant 2 mean osa revenue
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_mean_osa_revenue
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_mean_osa_revenue
      else ma.variant2_mean_osa_revenue
      end as variant2_mean_osa_revenue
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pct_change_mean_osa_revenue
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pct_change_mean_osa_revenue
      else ma.variant2_pct_change_mean_osa_revenue
      end as variant2_pct_change_mean_osa_revenue
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pval_mean_osa_revenue
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pval_mean_osa_revenue
      else ma.variant2_pval_mean_osa_revenue
      end as variant2_pval_mean_osa_revenue
   -- variant 2 LP Review Engagement 
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_lp_review_engagement
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_lp_review_engagement
      else ma.variant2_lp_review_engagement
      end as variant2_lp_review_engagement
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pct_change_lp_review_engagement
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pct_change_lp_review_engagement
      else ma.variant2_pct_change_lp_review_engagement
      end as variant2_pct_change_lp_review_engagement
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pval_lp_review_engagement
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pval_lp_review_engagement
      else ma.variant2_pval_lp_review_engagement
      end as variant2_pval_lp_review_engagement
 -- variant 2 LP Review Pagination 
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_lp_review_pagination
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_lp_review_pagination
      else ma.variant2_lp_review_pagination
      end as variant2_lp_review_pagination
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pct_change_lp_review_pagination
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pct_change_lp_review_pagination
      else ma.variant2_pct_change_lp_review_pagination
      end as variant2_pct_change_lp_review_pagination
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pval_lp_review_pagination
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pval_lp_review_pagination
      else ma.variant2_pval_lp_review_pagination
      end as variant2_pval_lp_review_pagination
   -- variant 2 LP Review Photo Opened 
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_lp_review_photo_opened
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_lp_review_photo_opened
      else ma.variant2_lp_review_photo_opened
      end as variant2_lp_review_photo_opened
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pct_change_lp_review_photo_opened
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pct_change_lp_review_photo_opened
      else ma.variant2_pct_change_lp_review_photo_opened
      end as variant2_pct_change_lp_review_photo_opened
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pval_lp_review_photo_opened
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pval_lp_review_photo_opened
      else ma.variant2_pval_lp_review_photo_opened
      end as variant2_pval_lp_review_photo_opened
  -- variant 2 LP Review Sort
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_lp_review_sort
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_lp_review_sort
      else ma.variant2_lp_review_sort
      end as variant2_lp_review_sort
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pct_change_lp_review_sort
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pct_change_lp_review_sort
      else ma.variant2_pct_change_lp_review_sort
      end as variant2_pct_change_lp_review_sort
  , case 
      when cgms.status = "Ramped Up" and variant = variant1_name then ma.variant2_pval_lp_review_sort
      when cgms.status = "Ramped Up" and variant = variant2_name then ma.variant1_pval_lp_review_sort
      else ma.variant2_pval_lp_review_sort
      end as variant2_pval_lp_review_sort   
from `etsy-data-warehouse-prod.etsy_atlas.catapult_gms_reports` cgms
left join metrics_agg ma
  on cgms.launch_id = ma.launch_id
  and cgms.start_date = date(timestamp_seconds(ma.start_date))
  and cgms.end_date = date(timestamp_seconds(ma.end_date))
where
  extract(year from cgms.end_date)>=2024
  and cgms.launch_id is not null
  and cgms.reviewed=1
)
-- this is the final aggregated product experiments table. In the metrics section it is overwriting the metric values if a KHM is entered in the KR metric 1/2 metric dropdowns
-- One thing to note is that there is not option to enter the metric value on the catapult report page, so we can not get the actual values for overwritten metrics     
select
  cgms.launch_id
  , concat("https://atlas.etsycorp.com/catapult/", CAST(cgms.launch_id AS STRING)) AS catapult_link
  , cgms.gms_report_id
  , cl.config_flag
  , cgms.noncatapult
  , cgms.is_long_term_holdout
  , cgms.experiment_name
  , cl.hypothesis
  , cgms.start_date
  , cgms.end_date
  , cgms.learnings
  , coalesce(cgms.initiative, cl.initiative) as initiative
  , cgms.subteam 
  , cgms.product_lead
  , cgms.analyst_lead
  , cgms.status
  , case -- this is updated to reflect the new guidance
      when (cgms.status = "Ramped Up")
            and (
                (ma.variant1_pval_conversion_rate< .05 and ma.variant1_pct_change_conversion_rate>0)
                or (ma.variant1_pval_opu< .05 and ma.variant1_pct_change_opu>0)
                or (ma.variant1_pval_mean_visits< .05 and ma.variant1_pct_change_mean_visits>0)
                or (ma.variant1_pval_winsorized_acxv< .05 and ma.variant1_pct_change_winsorized_acxv>0)
                or (ma.variant1_pval_gms_per_unit< .05 and ma.variant1_pct_change_gms_per_unit>0)  
                )
        then "Ramped Up KPI Win"
      when (cgms.status = "Ramped Up")
        then "Ramped Up Neutral"
      else "Ramped Down"
    end as ramp_decision
  , cgms.variant as ramped_variant
  , t.tag
  , t.group_tag
  , coalesce(p.platform,cgms.platform) as platform
  , coalesce(cgms.audience,cl.audience) AS audience
  , coalesce(cast(cgms.traffic_percent as FLOAT64),cast((cl.layer_end - cl.layer_start)/100 as FLOAT64),1) AS traffic_percentage
  , eca.gms_coverage
  , eca.traffic_coverage
  , eca.osa_coverage
  , eca.prolist_coverage
  , cgms.kpi_initiative_name
  , cgms.kpi_initiative_value
  , cgms.kpi_initiative_coverage
  , cgms.kr_metric_name
  , cgms.kr_metric_value
  , cgms.kr_metric_coverage
  , cgms.kr_metric_name_2 as kr2_metric_name
  , cgms.kr_metric_value_2 as kr2_metric_value
  , cgms.kr_metric_coverage_2 as kr2_metric_coverage
  -- Target metric 
  , ma.target_metric
  , ma.control_value_target_metric
  , ma.variant1_value_target_metric
  , ma.variant1_pct_change_target_metric
  , ma.variant1_pval_target_metric
  -- , case 
  --     when ma.variant1_pval_target_metric < 0.1 and end_date >= '2024-02-11' then 1 -- if the experiment ended before 2/11, 0.1 was the significance level used 
  --     when ma.variant1_pval_target_metric < 0.05 and end_date < '2024-02-11' then 1 -- if the experiment ended after 2/11, 0.05 was the significance level used 
  --     else 0 
  --   end as target_metric_is_significant
  , case when ma.variant1_pval_target_metric < 0.05 then 1 else 0 end as target_metric_is_significant
  -- Conversion rate
  , ma.control_conversion_rate
  , ma.variant1_conversion_rate
  , case 
      when LOWER(cgms.kr_metric_name) = 'conversion rate' then cgms.kr_metric_value
      when LOWER(cgms.kr_metric_name_2) = 'conversion rate' then cgms.kr_metric_value_2
      else ma.variant1_pct_change_conversion_rate
      end as variant1_pct_change_conversion_rate
  , ma.variant1_pval_conversion_rate
  , case when ma.variant1_pval_conversion_rate < 0.05 then 1 else 0 end as cr_is_significant
  --Percent with add to cart
  , ma.control_pct_atc
  , ma.variant1_pct_atc
  , case 
      when LOWER(cgms.kr_metric_name) = 'conversion rate' then cgms.kr_metric_value
      when LOWER(cgms.kr_metric_name_2) = 'conversion rate' then cgms.kr_metric_value_2
      else ma.variant1_pct_change_pct_atc
      end as variant1_pct_change_pct_atc
  , ma.variant1_pval_pct_atc
  , case when ma.variant1_pval_pct_atc < 0.05 then 1 else 0 end as atc_is_significant
  -- Listing view
  , ma.control_pct_listing_view
  , ma.variant1_pct_listing_view
  , case 
      when LOWER(cgms.kr_metric_name) = 'conversion rate' then cgms.kr_metric_value
      when LOWER(cgms.kr_metric_name_2) = 'conversion rate' then cgms.kr_metric_value_2
      else ma.variant1_pct_change_pct_listing_view
      end as variant1_pct_change_pct_listing_view
  , ma.variant1_pval_pct_listing_view
  , case when ma.variant1_pval_pct_listing_view < 0.05 then 1 else 0 end as listing_view_is_significant
 --Shop home 
  , ma.control_pct_w_shop_home_view
  , ma.variant1_pct_w_shop_home_view
  , case 
      when LOWER(cgms.kr_metric_name) = 'conversion rate' then cgms.kr_metric_value
      when LOWER(cgms.kr_metric_name_2) = 'conversion rate' then cgms.kr_metric_value_2
      else ma.variant1_pct_change_pct_w_shop_home_view
      end as variant1_pct_change_pct_w_shop_home_view
  , ma.variant1_pval_pct_w_shop_home_view
  , case when ma.variant1_pval_pct_w_shop_home_view < 0.05 then 1 else 0 end as shop_home_is_significant
  -- Mean visits
  , ma.control_mean_visits
  , ma.variant1_mean_visits
  , case 
      when LOWER(cgms.kr_metric_name) = 'mean visits' then cgms.kr_metric_value
      when LOWER(cgms.kr_metric_name_2) = 'mean visits' then cgms.kr_metric_value_2
      else ma.variant1_pct_change_mean_visits
      end as variant1_pct_change_mean_visits
  , ma.variant1_pval_mean_visits
  , case when ma.variant1_pval_mean_visits < 0.05 then 1 else 0 end as mean_visits_is_significant
  -- GMS per units
  , ma.control_gms_per_unit
  , ma.variant1_gms_per_unit
  , case 
      when LOWER(cgms.kr_metric_name) = 'gms per unit' then cgms.kr_metric_value
      when LOWER(cgms.kr_metric_name_2) = 'gms per unit' then cgms.kr_metric_value_2
      else ma.variant1_pct_change_gms_per_unit
      end as variant1_pct_change_gms_per_unit
  , ma.variant1_pval_gms_per_unit
  , case when ma.variant1_pval_gms_per_unit < 0.05 then 1 else 0 end as gms_per_unit_is_significant
  -- Mean engaged visits
  , ma.control_mean_engaged_visit
  , ma.variant1_mean_engaged_visit
  , case 
      when LOWER(cgms.kr_metric_name) = 'mean engaged_visit' then cgms.kr_metric_value
      when LOWER(cgms.kr_metric_name_2) = 'mean engaged_visit' then cgms.kr_metric_value_2
      else ma.variant1_pct_change_mean_engaged_visit
      end as variant1_pct_change_mean_engaged_visit
  , ma.variant1_pval_mean_engaged_visit
  , case when ma.variant1_pval_mean_engaged_visit < 0.05 then 1 else 0 end as engaged_visits_is_significant
  -- Winsorized ACxV
  , ma.control_winsorized_acxv
  , ma.variant1_winsorized_acxv
  , case 
      when LOWER(cgms.kr_metric_name) = 'winsorized ac*v' then cgms.kr_metric_value
      when LOWER(cgms.kr_metric_name_2) = 'winsorized ac*v' then cgms.kr_metric_value_2
      else ma.variant1_pct_change_winsorized_acxv
      end as variant1_pct_change_winsorized_acxv
  , ma.variant1_pval_winsorized_acxv
  , case when ma.variant1_pval_winsorized_acxv < 0.05 then 1 else 0 end as acxv_is_significant
  -- Orders per unit
  , ma.control_opu
  , ma.variant1_opu
  , case 
      when LOWER(cgms.kr_metric_name) = 'orders per unit' then cgms.kr_metric_value
      when LOWER(cgms.kr_metric_name_2) = 'orders per unit' then cgms.kr_metric_value_2
      else ma.variant1_pct_change_opu
      end as variant1_pct_change_opu
  , ma.variant1_pval_opu
  , case when ma.variant1_pval_opu < 0.05 then 1 else 0 end as opu_is_significant
  -- Winsorized AOV
  , ma.control_aov
  , ma.variant1_aov
  , case 
      when LOWER(cgms.kr_metric_name) = 'winsorized aov' then cgms.kr_metric_value
      when LOWER(cgms.kr_metric_name_2) = 'winsorized aov' then cgms.kr_metric_value_2
      else ma.variant1_pct_change_aov
      end as variant1_pct_change_aov
  , ma.variant1_pval_aov
  , case when ma.variant1_pval_aov < 0.05 then 1 else 0 end as aov_is_significant
  -- ADs Conversion rate
  , ma.control_ads_cvr
  , ma.variant1_ads_cvr
  , case 
      when LOWER(cgms.kr_metric_name) = 'ads conversion rate' then cgms.kr_metric_value
      when LOWER(cgms.kr_metric_name_2) = 'ads conversion rate' then cgms.kr_metric_value_2
      else ma.variant1_pct_change_ads_cvr
      end as variant1_pct_change_ads_cvr
  , ma.variant1_pval_ads_cvr
  , case when ma.variant1_pval_ads_cvr < 0.05 then 1 else 0 end as ads_cvr_is_significant
  -- ADs ACxV
  , ma.control_ads_acxv
  , ma.variant1_ads_acxv
  , case 
      when LOWER(cgms.kr_metric_name) = 'ads winsorized ac*v ($100)' then cgms.kr_metric_value
      when LOWER(cgms.kr_metric_name_2) = 'ads winsorized ac*v ($100)' then cgms.kr_metric_value_2
      else ma.variant1_pct_change_ads_acxv
      end as variant1_pct_change_ads_acxv
  , ma.variant1_pval_ads_acxv
  , case when ma.variant1_pval_ads_acxv < 0.05 then 1 else 0 end as ads_acxv_is_significant
  -- Mean prolist spend
  , ma.control_mean_prolist_spend
  , ma.variant1_mean_prolist_spend
  , case 
      when LOWER(cgms.kr_metric_name) = 'etsy ads click revenue' then cgms.kr_metric_value
      when LOWER(cgms.kr_metric_name_2) = 'etsy ads click revenue' then cgms.kr_metric_value_2
      else ma.variant1_pct_change_mean_prolist_spend
      end as variant1_pct_change_mean_prolist_spend
  , ma.variant1_pval_mean_prolist_spend
  , case when ma.variant1_pval_mean_prolist_spend < 0.05 then 1 else 0 end as prolist_spend_is_significant
  -- Mean osa revenue
  , ma.control_mean_osa_revenue
  , ma.variant1_mean_osa_revenue
  , case 
      when LOWER(cgms.kr_metric_name) = 'offsite ads attributed revenue' then cgms.kr_metric_value
      when LOWER(cgms.kr_metric_name_2) = 'offsite ads attributed revenue' then cgms.kr_metric_value_2
      else ma.variant1_pct_change_mean_osa_revenue
      end as variant1_pct_change_mean_osa_revenue
  , ma.variant1_pval_mean_osa_revenue
  , case when ma.variant1_pval_mean_osa_revenue < 0.05 then 1 else 0 end as osa_is_significant
    -- LP Review Engagement
  , ma.control_lp_review_engagement
  , ma.variant1_lp_review_engagement
  , ma.variant1_pct_change_lp_review_engagement
  , ma.variant1_pval_lp_review_engagement
  , case when ma.variant1_pval_lp_review_engagement < 0.05 then 1 else 0 end as lp_review_engagement_is_significant
    --  LP Review Pagination 
  , ma.control_lp_review_pagination
  , ma.variant1_lp_review_pagination
  , ma.variant1_pct_change_lp_review_pagination
  , ma.variant1_pval_lp_review_pagination
  , case when ma.variant1_pval_lp_review_pagination < 0.05 then 1 else 0 end as lp_review_pagination_is_significant
    --  LP Review Photo Opened 
  , ma.control_lp_review_photo_opened
  , ma.variant1_lp_review_photo_opened
  , ma.variant1_pct_change_lp_review_photo_opened
  , ma.variant1_pval_lp_review_photo_opened
  , case when ma.variant1_pval_lp_review_photo_opened < 0.05 then 1 else 0 end as lp_review_photo_opened_is_significant
    --  LP Review Sort
  , ma.control_lp_review_sort
  , ma.variant1_lp_review_sort
  , ma.variant1_pct_change_lp_review_sort
  , ma.variant1_pval_lp_review_sort
  , case when ma.variant1_pval_lp_review_sort < 0.05 then 1 else 0 end as lp_review_sort_is_significant
  -- Variant 2 Conversion rate
  , ma.variant2_conversion_rate
  , ma.variant2_pct_change_conversion_rate
  , ma.variant2_pval_conversion_rate
  -- Variant 2 Mean visits
  , ma.variant2_mean_visits
  , ma.variant2_pct_change_mean_visits
  , ma.variant2_pval_mean_visits
  -- Variant 2 GMS per unit
  , ma.variant2_gms_per_unit
  , ma.variant2_pct_change_gms_per_unit
  , ma.variant2_pval_gms_per_unit
  -- Variant Mean engaged visit
  , ma.variant2_mean_engaged_visit
  , ma.variant2_pct_change_mean_engaged_visit
  , ma.variant2_pval_mean_engaged_visit
  -- Variant 2 Winsorized ACxV
  , ma.variant2_winsorized_acxv
  , ma.variant2_pct_change_winsorized_acxv
  , ma.variant2_pval_winsorized_acxv
  -- Variant 2 Orders per unit
  , ma.variant2_opu
  , ma.variant2_pct_change_opu
  , ma.variant2_pval_opu  
  -- Variant 2 AOV
  , ma.variant2_aov
  , ma.variant2_pct_change_aov
  , ma.variant2_pval_aov
  -- Variant 2 ADs ACxV
  , ma.variant2_ads_acxv
  , ma.variant2_pct_change_ads_acxv
  , ma.variant2_pval_ads_acxv
  -- Variant 2 ADs Conversion rate
  , ma.variant2_ads_cvr
  , ma.variant2_pct_change_ads_cvr
  , ma.variant2_pval_ads_cvr
  -- Variant 2 Mean prolist spend
  , ma.variant2_mean_prolist_spend
  , ma.variant2_pct_change_mean_prolist_spend
  , ma.variant2_pval_mean_prolist_spend
  -- Variant 2 Mean OSA revenue
  , ma.variant2_mean_osa_revenue
  , ma.variant2_pct_change_mean_osa_revenue
  , ma.variant2_pval_mean_osa_revenue
    -- Variant 2 LP Review Engagement
  , ma.variant2_lp_review_engagement
  , ma.variant2_pct_change_lp_review_engagement
  , ma.variant2_pval_lp_review_engagement
  -- Variant 2 LP Review Pagination
  , ma.variant2_lp_review_pagination
  , ma.variant2_pct_change_lp_review_pagination
  , ma.variant2_pval_lp_review_pagination
  -- Variant 2 LP Review photo_opened
  , ma.variant2_lp_review_photo_opened
  , ma.variant2_pct_change_lp_review_photo_opened
  , ma.variant2_pval_lp_review_photo_opened
  -- Variant 2 LP Review Sort
  , ma.variant2_lp_review_sort
  , ma.variant2_pct_change_lp_review_sort
  , ma.variant2_pval_lp_review_sort
  -- discounted metric fields
  , cgms.conv_pct_change as reported_gms_change_perc
  , cgms.gms_coverage as reported_gms_coverage
from `etsy-data-warehouse-prod.etsy_atlas.catapult_gms_reports`  cgms
left join `etsy-data-warehouse-prod.etsy_atlas.catapult_launches` cl
  on cgms.launch_id = cl.launch_id
left join `etsy-data-warehouse-prod.rollups.experiment_tags` t
  on TRIM(REGEXP_EXTRACT(LOWER(cgms.learnings), 'experiment-tags:([^\n\\.]+)')) = TRIM(LOWER(t.tag))  
left join plats_agg p 
  on cgms.launch_id = p.launch_id
left join exp_coverage_agg eca
  on cgms.launch_id = eca.launch_id
  and cgms.start_date = eca.start_date
  and cgms.end_date = eca.end_date
left join metrics_agg_clean ma 
  on cgms.launch_id = ma.launch_id
  and cgms.start_date = date(timestamp_seconds(ma.start_date))
  and cgms.end_date = date(timestamp_seconds(ma.end_date))
where
  extract(year from cgms.end_date)>=2024
  and cgms.reviewed=1
  and cgms.start_date<current_date-1 -- there's an experiment with a strange date
  and coalesce(cgms.initiative, cl.initiative) in ('Drive Conversion')
  and cgms.subteam in ('RegX','Registry Experience')
);
