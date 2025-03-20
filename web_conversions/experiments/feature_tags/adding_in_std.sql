/* Alisha's master code: 
https://github.com/etsy/apeermohamed/blob/6096040519b7e3fe8d6dbf8b27e0a30ca655db55/CBX%20Home/experiment_scripts/home_experiments_by_custom_segmentations.sql#L304-L307*/ 

--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- DESKTOP
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Define variables
DECLARE config_flag_param STRING DEFAULT "growth_regx.lp_review_feature_tags_desktop";
DECLARE start_date DATE;
DECLARE end_date DATE;

-- Get experiment's start date and end date
SET (start_date, end_date) = (
  SELECT AS STRUCT
    MAX(DATE(boundary_start_ts)) AS start_date,
    MAX(_date) AS end_date,
  FROM
    `etsy-data-warehouse-prod.catapult_unified.experiment`
  WHERE
    experiment_id = config_flag_param
);

-- Get experiment's bucketed units
CREATE OR REPLACE TEMPORARY TABLE xp_units AS (
  SELECT 
    bucketing_id,
    variant_id,
    bucketing_ts
  FROM
    `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
  WHERE
    _date = end_date
    AND experiment_id = config_flag_param
);

-- Get experiment's bucketed visits
CREATE OR REPLACE TEMPORARY TABLE xp_visits AS (
  SELECT
    v.visit_id,
    xp.bucketing_id
  FROM
    `etsy-data-warehouse-prod.weblog.visits` AS v
  INNER JOIN
    xp_units AS xp
      ON
        xp.bucketing_id = v.browser_id
        AND TIMESTAMP_TRUNC(xp.bucketing_ts, SECOND) <= v.end_datetime
  WHERE
    v._date BETWEEN start_date AND end_date
);
/* HERE, RECREATE METRICS IN CATAPULT USING EVENT FILTER */
-- Get browsers who saw the top of the reviews section
CREATE OR REPLACE TEMPORARY TABLE browsers_with_key_event AS (
  SELECT DISTINCT
    v.bucketing_id
  FROM
    `etsy-data-warehouse-prod.weblog.events` AS e
  INNER JOIN 
    xp_visits AS v USING(visit_id)
  WHERE
    e._date BETWEEN start_date AND end_date
    AND e.event_type = "listing_page_reviews_container_top_seen" -- event fires when a browser sees the top of the review section  
);

-- Get KHM aggregated events for experiment's bucketed units
CREATE OR REPLACE TEMPORARY TABLE xp_khm_agg_events AS (
  SELECT
    xp.bucketing_id,
    xp.variant_id,
    e.event_id,
    e.event_type,
    e.event_value
  FROM
    `etsy-data-warehouse-prod.catapult_unified.aggregated_event_daily` AS e
  INNER JOIN
    xp_units AS xp USING (bucketing_id)
  WHERE
    e._date BETWEEN start_date AND end_date
    AND e.experiment_id = config_flag_param
    AND e.event_id IN (
      "backend_cart_payment",
        "bounce",
        "backend_add_to_cart", 
        "checkout_start",  
        "engaged_visit",
        "visits",
        "completed_checkouts",
        "page_count",
        "total_winsorized_gms",
        "total_winsorized_order_value"
      )
    AND e.bucketing_id_type = 1 -- browser_id
);

-- Get KHM aggregated events for experiment's bucketed units by unit
CREATE OR REPLACE TEMPORARY TABLE xp_khm_agg_events_by_unit AS (
  SELECT
    bucketing_id,
    -- metrics
    SUM(IF(event_id = "backend_cart_payment", event_value, 0)) AS orders,
    SUM(IF(event_id = "bounce", event_value, 0)) AS bounced_visits,
    COUNTIF(event_id = "backend_add_to_cart") AS atc_count,
    COUNTIF(event_id = "checkout_start") AS checkout_start_count,
    SUM(IF(event_id = "engaged_visit", event_value, 0)) AS engaged_visits,
    SUM(IF(event_id = "visits", event_value, 0)) AS visits,
    SUM(IF(event_id = "completed_checkouts", event_value, 0)) AS completed_checkouts,
    SUM(IF(event_id = "page_count", event_value, 0)) AS page_count,
    SUM(IF(event_id = "total_winsorized_gms", event_value, 0)) AS winsorized_gms,
    SUM(IF(event_id = "total_winsorized_order_value", event_value, 0)) AS winsorized_order_value_sum,
    --max for sd calcs 
    max(case when event_id = 'backend_cart_payment' then event_value else 0 end) as total_orders, -- total orders per unit
    max(case when event_id = 'total_winsorized_gms' and event_value > 0 then event_value else null end) as converting_unit_value, -- ac*v (acbv)
    max(case when event_id = 'backend_cart_payment' then 1 else 0 end) as converted,-- conversion rate
    max(case when event_id = 'visits' then event_value else null end) as total_visits, -- mean visits
  FROM
    xp_khm_agg_events
  GROUP BY
    1
);

-- Key Health Metrics (Winsorized ACBV and AOV) - Total (To compare with Catapult as a sanity check)
create or replace table etsy-data-warehouse-dev.madelinecollins.xp_feature_tags_desktop_std as (
SELECT
  xp.variant_id,
  COUNT(xp.bucketing_id) AS browsers,
  -- metrics
  SAFE_DIVIDE(COUNTIF(e.orders > 0), COUNT(xp.bucketing_id)) AS conversion_rate,
  SAFE_DIVIDE(COUNTIF(e.bounced_visits > 0), COUNT(xp.bucketing_id)) AS bounce_rate,
  SAFE_DIVIDE(COUNTIF(e.atc_count > 0), COUNT(xp.bucketing_id)) AS pct_with_atc,
  SAFE_DIVIDE(COUNTIF(e.checkout_start_count > 0), COUNT(xp.bucketing_id)) AS pct_with_checkout_start,
  SAFE_DIVIDE(SUM(e.engaged_visits), COUNT(xp.bucketing_id)) AS mean_engaged_visits,
  SAFE_DIVIDE(SUM(e.visits), COUNT(xp.bucketing_id)) AS mean_visits,
  SAFE_DIVIDE(SUM(e.orders), COUNTIF(e.orders > 0)) AS ocb,
  SAFE_DIVIDE(SUM(e.completed_checkouts), COUNT(xp.bucketing_id)) AS orders_per_browser,
  SAFE_DIVIDE(SUM(e.page_count), COUNT(xp.bucketing_id)) AS pages_per_browser,
  SAFE_DIVIDE(SUM(e.winsorized_gms), COUNTIF(e.orders > 0)) AS winsorized_acbv,
  SAFE_DIVIDE(SUM(e.winsorized_order_value_sum), SUM(e.completed_checkouts)) AS winsorized_aov,
  --browser counts
  COUNTIF(e.orders > 0) AS converted_browsers,
  COUNTIF(e.atc_count > 0) AS atc_browsers,
  -- standard deviation of metrics
  stddev(e.orders) as sd_conversion_rate, 
  stddev(e.atc_count) as sd_atc,
  stddev(e.visits) as sd_visits,
  stddev(e.engaged_visits) as sd_engaged_visits,
  stddev(e.completed_checkouts) as sd_total_orders_per_browser, 
  stddev(e.winsorized_gms) as sd_acbv,  
FROM
  xp_units AS xp
LEFT JOIN
  xp_khm_agg_events_by_unit AS e USING (bucketing_id)
GROUP BY
  1
ORDER BY
  1);

  -- Key Health Metrics (Winsorized ACBV and AOV) - Only browsers who viewed a listing page with review photos
create or replace table etsy-data-warehouse-dev.madelinecollins.xp_feature_tags_desktop_filtered_std as (
SELECT
  xp.variant_id,
  COUNT(xp.bucketing_id) AS browsers,
  -- metrics
  SAFE_DIVIDE(COUNTIF(e.orders > 0), COUNT(xp.bucketing_id)) AS conversion_rate,
  SAFE_DIVIDE(COUNTIF(e.bounced_visits > 0), COUNT(xp.bucketing_id)) AS bounce_rate,
  SAFE_DIVIDE(COUNTIF(e.atc_count > 0), COUNT(xp.bucketing_id)) AS pct_with_atc,
  SAFE_DIVIDE(COUNTIF(e.checkout_start_count > 0), COUNT(xp.bucketing_id)) AS pct_with_checkout_start,
  SAFE_DIVIDE(SUM(e.engaged_visits), COUNT(xp.bucketing_id)) AS mean_engaged_visits,
  SAFE_DIVIDE(SUM(e.visits), COUNT(xp.bucketing_id)) AS mean_visits,
  SAFE_DIVIDE(SUM(e.orders), COUNTIF(e.orders > 0)) AS ocb,
  SAFE_DIVIDE(SUM(e.completed_checkouts), COUNT(xp.bucketing_id)) AS orders_per_browser,
  SAFE_DIVIDE(SUM(e.page_count), COUNT(xp.bucketing_id)) AS pages_per_browser,
  SAFE_DIVIDE(SUM(e.winsorized_gms), COUNTIF(e.orders > 0)) AS winsorized_acbv,
  SAFE_DIVIDE(SUM(e.winsorized_order_value_sum), SUM(e.completed_checkouts)) AS winsorized_aov,
  --browser counts
  COUNTIF(e.orders > 0) AS converted_browsers,
  COUNTIF(e.atc_count > 0) AS atc_browsers,
  -- standard deviation of metrics
  stddev(e.orders) as sd_conversion_rate, 
  stddev(e.atc_count) as sd_atc,
  stddev(e.visits) as sd_visits,
  stddev(e.engaged_visits) as sd_engaged_visits,
  stddev(e.completed_checkouts) as sd_total_orders_per_browser, 
  stddev(e.winsorized_gms) as sd_acbv,  
FROM
  xp_units AS xp
INNER JOIN
  browsers_with_key_event AS b USING (bucketing_id)
LEFT JOIN
  xp_khm_agg_events_by_unit AS e USING (bucketing_id)
GROUP BY ALL
ORDER BY
  1);




----- pvals
with sd_metrics as (
select 
  'Bucketed units' as metric,
  null as n_off, 
  null as n_on, 
  sum(case when variant_id = 'off' then browsers end) as mean_off, 
  sum(case when variant_id = 'on' then browsers end) as mean_on,
  null as pct_diff,
  null as sd_off, 
  null as sd_on
from etsy-data-warehouse-dev.madelinecollins.xp_feature_tags_desktop_std
group by all
union all 
-- conversion rate 
select 
  'Conversion Rate' as metric,
  sum(case when variant_id = 'off' then browsers end) as n_off, 
  sum(case when variant_id = 'on' then browsers end) as n_on,
  sum(case when variant_id = 'off' then conversion_rate end) as mean_off, 
  sum(case when variant_id = 'on' then conversion_rate end) as mean_on,
  sum(case when variant_id = 'off' then conversion_rate end) / sum(case when variant_id = 'on' then conversion_rate end)  - 1 as pct_diff,
  sum(case when variant_id = 'off' then sd_conversion_rate end) as sd_off,
  sum(case when variant_id = 'on' then sd_conversion_rate end) as sd_on
from etsy-data-warehouse-dev.madelinecollins.xp_feature_tags_desktop_std
union all 
-- acbv
select 
  'ACBV' as metric,
  sum(case when variant_id = 'off' then browsers end) as n_off, 
  sum(case when variant_id = 'on' then browsers end) as n_on,
  sum(case when variant_id = 'off' then winsorized_acbv end) as mean_off, 
  sum(case when variant_id = 'on' then winsorized_acbv end) as mean_on,
  sum(case when variant_id = 'off' then winsorized_acbv end) / sum(case when variant_id = 'on' then winsorized_acbv end)  - 1 as pct_diff,
  sum(case when variant_id = 'off' then sd_acbv end) as sd_off,
  sum(case when variant_id = 'on' then sd_acbv end) as sd_on
from etsy-data-warehouse-dev.madelinecollins.xp_feature_tags_desktop_std
union all 
-- ATC 
select 
  'ATC' as metric,
  sum(case when variant_id = 'off' then browsers end) as n_off, 
  sum(case when variant_id = 'on' then browsers end) as n_on,
  sum(case when variant_id = 'off' then pct_with_atc end) as mean_off, 
  sum(case when variant_id = 'on' then pct_with_atc end) as mean_on,
  sum(case when variant_id = 'off' then pct_with_atc end) / sum(case when variant_id = 'on' then pct_with_atc end)  - 1 as pct_diff,
  sum(case when variant_id = 'off' then sd_atc end) as sd_off,
  sum(case when variant_id = 'on' then sd_atc end) as sd_on
from etsy-data-warehouse-dev.madelinecollins.xp_feature_tags_desktop_std
)
select 
  metric, 
  mean_off, 
  mean_on, 
  pct_diff,
  case when metric = 'Bucketed units' then null else `etsy-data-warehouse-prod.functions.t_test_agg`(n_off, n_on, mean_off, mean_on, sd_off, sd_on).p_value end as p_value, 
  case when metric = 'Bucketed units' then null else
    case when `etsy-data-warehouse-prod.functions.t_test_agg`(n_off, n_on, mean_off, mean_on, sd_off, sd_on).p_value <= 0.05 then 'Yes' 
    else 'No' end end as stat_sig 
from sd_metrics 
