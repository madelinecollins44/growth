/* This experiment added a section ingress under the listing pagination on shop home. 
This analysis is meant to figure out if some channels responded better to this. 
-- desktop: https://atlas.etsycorp.com/catapult/1361091594266 (growth_regx.sh_section_ingresses_under_pagination_desktop)
-- mobile web: https://atlas.etsycorp.com/catapult/1361101148193 (growth_regx.sh_section_ingresses_under_pagination_mweb) */
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- DESKTOP
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Define variables
DECLARE config_flag_param STRING DEFAULT "growth_regx.sh_section_ingresses_under_pagination_desktop";
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
    bucketing_ts,
    visit_id,
    top_channel,
    start_datetime as visit_start,
    row_number () over (partition by bucketing_id order by visit_id asc) as visit_order
  FROM
    `etsy-data-warehouse-prod.catapult_unified.bucketing_period` xp
  INNER JOIN 
    `etsy-data-warehouse-prod.weblog.visits` AS v
   ON
      xp.bucketing_id = v.browser_id
      AND TIMESTAMP_TRUNC(xp.bucketing_ts, SECOND) <= v.end_datetime
  WHERE
    xp._date = end_date
    AND experiment_id = config_flag_param
    AND     v._date BETWEEN start_date AND end_date
QUALIFY row_number () over (partition by bucketing_id order by visit_id asc) = 1 -- only grab the channel from the first visit
);

-- Get experiment's bucketed visits
CREATE OR REPLACE TEMPORARY TABLE xp_visits AS (
  SELECT
    v.visit_id,
    xp.bucketing_id,
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
-- Get browsers who saw the listing grid
CREATE OR REPLACE TEMPORARY TABLE browsers_with_key_event AS (
  SELECT DISTINCT
    v.top_channel,
    v.bucketing_id
  FROM
    `etsy-data-warehouse-prod.weblog.events` AS e
  INNER JOIN 
    xp_visits AS v USING(visit_id)
  WHERE
    e._date BETWEEN start_date AND end_date
    AND e.event_type = "shop_home_listing_grid_seen" -- event fires when a browser sees the listing grid 
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
    SUM(IF(event_id = "backend_cart_payment", event_value, 0)) AS orders,
    SUM(IF(event_id = "bounce", event_value, 0)) AS bounced_visits,
    COUNTIF(event_id = "backend_add_to_cart") AS atc_count,
    COUNTIF(event_id = "checkout_start") AS checkout_start_count,
    SUM(IF(event_id = "engaged_visit", event_value, 0)) AS engaged_visits,
    SUM(IF(event_id = "visits", event_value, 0)) AS visits,
    SUM(IF(event_id = "completed_checkouts", event_value, 0)) AS completed_checkouts,
    SUM(IF(event_id = "page_count", event_value, 0)) AS page_count,
    SUM(IF(event_id = "total_winsorized_gms", event_value, 0)) AS winsorized_gms,
    SUM(IF(event_id = "total_winsorized_order_value", event_value, 0)) AS winsorized_order_value_sum
  FROM
    xp_khm_agg_events
  GROUP BY
    1
);

-- Key Health Metrics (Winsorized ACBV and AOV) - Total (To compare with Catapult as a sanity check)
create or replace table etsy-data-warehouse-dev.madelinecollins.xp_feature_tags_desktop as (
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
  COUNTIF(e.atc_count > 0) AS atc_browsers
FROM
  xp_units AS xp
LEFT JOIN
  xp_khm_agg_events_by_unit AS e USING (bucketing_id)
GROUP BY
  1
ORDER BY
  1);

-- Key Health Metrics (Winsorized ACBV and AOV) - Only browsers who viewed a listing page with review photos
create or replace table etsy-data-warehouse-dev.madelinecollins.xp_section_ingress_desktop_filtered as (
SELECT
  xp.variant_id,
  b.top_channel,
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
  COUNTIF(e.atc_count > 0) AS atc_browsers
FROM
  xp_units AS xp
INNER JOIN
  browsers_with_key_event AS b USING (bucketing_id)
LEFT JOIN
  xp_khm_agg_events_by_unit AS e USING (bucketing_id)
GROUP BY ALL
ORDER BY
  1);


-- how many bucketed browsers saw the treatment? 
select
  count(distinct v.bucketing_id) as bucketed_browsers,
  count(distinct e.bucketing_id) as browsers_saw_treatment,
  count(distinct e.bucketing_id) / count(distinct v.bucketing_id) as share_browsers_saw_treatment
from 
  etsy-bigquery-adhoc-prod._script1cdcf08310b30c3142493312323f1f24251080b7.xp_visits v
left join 
  etsy-bigquery-adhoc-prod._script1cdcf08310b30c3142493312323f1f24251080b7.browsers_with_key_event e using (bucketing_id)
------------------------------------------------------------------------------------------
-- TESTING
------------------------------------------------------------------------------------------
--make sure bucketing_id + visit are still unique
select visit_id, bucketing_id, count(*) from etsy-bigquery-adhoc-prod._script1cdcf08310b30c3142493312323f1f24251080b7.xp_visits group by all order by 3 desc limit 10

--find a browser w/ a high visit count to check visit order
select bucketing_id, count(visit_id) from etsy-bigquery-adhoc-prod._script1cdcf08310b30c3142493312323f1f24251080b7.xp_visits group by all order by 2 desc  limit 10

--does ordering work? 
select * from etsy-bigquery-adhoc-prod._script1cdcf08310b30c3142493312323f1f24251080b7.xp_visits where bucketing_id in ('eUu6shzIyoyHizRX4lFJZTUtm46n') order by visit_order asc

--make sure the top channel carried over properly
select top_channel, bucketing_id, count(*) from etsy-bigquery-adhoc-prod._script1cdcf08310b30c3142493312323f1f24251080b7.browsers_with_key_event group by all order by 3 desc limit 10
select bucketing_id, count(*) from etsy-bigquery-adhoc-prod._script1cdcf08310b30c3142493312323f1f24251080b7.browsers_with_key_event group by all order by 2 desc limit 10
select bucketing_id, count(distinct top_channel), count(top_channel) from etsy-bigquery-adhoc-prod._script1cdcf08310b30c3142493312323f1f24251080b7.browsers_with_key_event group by all order by 3 desc 
select top_channel, count(distinct bucketing_id), count(bucketing_id) from etsy-bigquery-adhoc-prod._script1cdcf08310b30c3142493312323f1f24251080b7.browsers_with_key_event group by all order by 3 desc 

--make sure browser from browsers_with_key_event matches first channel from xp_visits 
