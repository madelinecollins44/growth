--------------------------------------------------------------------------------------------------------------------------------------------------------------------
--This experiment added a "view all" option to the images in the review section on the listing page. 
-- This experiment was not event filtered, so we currently dont have data on how browsers that saw the change (fired a review_seen event) engaged. 
-- desktop: https://atlas.etsycorp.com/catapult/1345084581266
-- mobile web: https://atlas.etsycorp.com/catapult/1344991421721
--------------------------------------------------------------------------------------------------------------------------------------------------------------------


--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- DESKTOP
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Define variables
DECLARE config_flag_param STRING DEFAULT "growth_regx.lp_review_photos_view_all_link_desktop";
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

-- Get browsers who viewed a listing page with review photos
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
create or replace table etsy-data-warehouse-dev.madelinecollins.total_lp_review_photos_view_all_link_desktop as (
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
  SAFE_DIVIDE(SUM(e.winsorized_order_value_sum), SUM(e.orders)) AS winsorized_aov,
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
create or replace table etsy-data-warehouse-dev.madelinecollins.filtered_lp_review_photos_view_all_link_desktop as (
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
  SAFE_DIVIDE(SUM(e.winsorized_order_value_sum), SUM(e.orders)) AS winsorized_aov,
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

-- z score calc
begin
create temporary table p_values as
with browser_count as 
(select
  case when variant_id = 'on' then converted_browsers end as cr_browsers_t,
  case when variant_id = 'on' then browsers end as browsers_t,
  case when variant_id = 'off' then converted_browsers end as cr_browsers_c,
  case when variant_id = 'off' then browsers end as browsers_c,
from 
  etsy-data-warehouse-dev.madelinecollins.filtered_lp_review_photos_view_all_link_desktop
)
, z_values as (
  select 
  (cr_browsers_t / browsers_t) - (cr_browsers_c / browsers_c) as num,
  ((cr_browsers_t+cr_browsers_c) / (browsers_t+browsers_c)) * (1-(cr_browsers_t+cr_browsers_c)/(browsers_t+browsers_c)) as denom1,
  (1/browsers_c) + (1/browsers_t) as denom2
from 
  browser_count
  )
select 
  abs(num/(sqrt(denom1*denom2))) as z_score -- if z-score is above 1.64 it's significant
from z_values
;
end

--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- MOBILE WEB 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Define variables
DECLARE config_flag_param STRING DEFAULT "growth_regx.lp_review_photos_view_all_link_mweb";
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

-- Get browsers who viewed a listing page with review photos
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
SELECT
  xp.variant_id,
  COUNT(xp.bucketing_id) AS browsers,
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
  SAFE_DIVIDE(SUM(e.winsorized_order_value_sum), SUM(e.orders)) AS winsorized_aov
FROM
  xp_units AS xp
LEFT JOIN
  xp_khm_agg_events_by_unit AS e USING (bucketing_id)
GROUP BY
  1
ORDER BY
  1;

-- Key Health Metrics (Winsorized ACBV and AOV) - Only browsers who viewed a listing page with review photos
SELECT
  xp.variant_id,
  COUNT(xp.bucketing_id) AS browsers,
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
  SAFE_DIVIDE(SUM(e.winsorized_order_value_sum), SUM(e.orders)) AS winsorized_aov
FROM
  xp_units AS xp
INNER JOIN
  browsers_with_key_event AS b USING (bucketing_id)
LEFT JOIN
  xp_khm_agg_events_by_unit AS e USING (bucketing_id)
GROUP BY ALL
ORDER BY
  1;
