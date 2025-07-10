/* This experiment added a section ingress under the listing pagination on shop home. 
This analysis is meant to figure out if some channels responded better to this. 
-- desktop: https://atlas.etsycorp.com/catapult/1361091594266 (growth_regx.sh_section_ingresses_under_pagination_desktop)
-- mobile web: https://atlas.etsycorp.com/catapult/1361101148193 (growth_regx.sh_section_ingresses_under_pagination_mweb) */

--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- DESKTOP
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Define variables
DECLARE config_flag_param STRING DEFAULT "local_pe.q2_2025.buyer_trust_accelerator.browser";
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
   date(bucketing_ts) as bucketing_date,
  FROM
    `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
  WHERE
    _date = end_date
    AND experiment_id = config_flag_param
);

CREATE OR REPLACE TEMPORARY TABLE xp_khm_segments AS (
    WITH first_bucket_segments_unpivoted AS (
        SELECT
            bucketing_id,
            variant_id,
            event_id,
            event_value
        FROM
            `etsy-data-warehouse-prod.catapult_unified.aggregated_segment_event`
        WHERE
            _date = end_date
            AND experiment_id = config_flag_param
            AND event_id IN (
                "buyer_segment"
            )
    )
    SELECT
        *
    FROM
        first_bucket_segments_unpivoted
    PIVOT(
        MAX(event_value)
        FOR event_id IN (
            "buyer_segment"
        )
    )
);

CREATE OR REPLACE TEMPORARY TABLE xp_units_segments AS (
  SELECT 
    u.bucketing_id,
    u.variant_id,
    u.bucketing_ts,
    u.bucketing_date,
    buyer_segment,
  FROM
    xp_units u
  JOIN 
    xp_khm_segments 
      using (bucketing_id)
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
create or replace table etsy-data-warehouse-dev.madelinecollins.pe_habitual_metrics as (
SELECT
  xp.variant_id,
  -- xp.bucketing_date,
  --case when lower(xp.buyer_segment) in ('habitual') then 1 else 0 end as habitual_browser,
  COUNT(xp.bucketing_id) AS browsers,
  -- metrics
  SAFE_DIVIDE(COUNTIF(e.orders > 0), COUNT(xp.bucketing_id)) AS conversion_rate,
  -- SAFE_DIVIDE(COUNTIF(e.bounced_visits > 0), COUNT(xp.bucketing_id)) AS bounce_rate,
  -- SAFE_DIVIDE(COUNTIF(e.atc_count > 0), COUNT(xp.bucketing_id)) AS pct_with_atc,
  -- SAFE_DIVIDE(COUNTIF(e.checkout_start_count > 0), COUNT(xp.bucketing_id)) AS pct_with_checkout_start,
  -- SAFE_DIVIDE(SUM(e.engaged_visits), COUNT(xp.bucketing_id)) AS mean_engaged_visits,
  -- SAFE_DIVIDE(SUM(e.visits), COUNT(xp.bucketing_id)) AS mean_visits,
  SAFE_DIVIDE(SUM(e.orders), COUNTIF(e.orders > 0)) AS ocb,
  SAFE_DIVIDE(SUM(e.completed_checkouts), COUNT(xp.bucketing_id)) AS orders_per_browser,
  -- SAFE_DIVIDE(SUM(e.page_count), COUNT(xp.bucketing_id)) AS pages_per_browser,
  SAFE_DIVIDE(SUM(e.winsorized_gms), COUNTIF(e.completed_checkouts > 0)) AS winsorized_acbv,
  SAFE_DIVIDE(SUM(e.winsorized_order_value_sum), SUM(e.completed_checkouts)) AS winsorized_aov,
FROM
  xp_units AS xp
LEFT JOIN
  xp_khm_agg_events_by_unit AS e USING (bucketing_id)
GROUP BY ALL
ORDER BY
  1);

