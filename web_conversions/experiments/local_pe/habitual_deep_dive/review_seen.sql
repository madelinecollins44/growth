
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

-- Get the first bucketing moment for each experimental unit (e.g. browser or user).
CREATE OR REPLACE TEMPORARY TABLE ab_first_bucket AS (
    SELECT
        bucketing_id,
        variant_id,
        MIN(bucketing_ts) AS bucketing_ts,
    FROM
        `etsy-data-warehouse-prod.catapult_unified.bucketing`
    WHERE
        _date BETWEEN start_date AND end_date
        AND experiment_id = config_flag_param
    GROUP BY
        bucketing_id, variant_id
);


-- Get segment values based on first bucketing moment.
CREATE OR REPLACE TEMPORARY TABLE first_bucket_segments_unpivoted AS (
    SELECT
        a.bucketing_id,
        a.bucketing_ts,
        a.variant_id,
        s.event_id,
        s.event_value,
    FROM
        ab_first_bucket a 
    JOIN
        `etsy-data-warehouse-prod.catapult_unified.segment_event_custom` s
        USING(bucketing_id, bucketing_ts)
    WHERE
        s._date BETWEEN start_date AND end_date
        AND s.experiment_id = config_flag_param
        AND s.event_id IN (
            "buyer_segment"
        )
);

CREATE OR REPLACE TEMPORARY TABLE first_bucket_segments AS (
    SELECT
        *
    FROM
        first_bucket_segments_unpivoted
    PIVOT(
        MAX(event_value)
        FOR event_id IN (
            "buyer_segment"        )
    )
);

/*
-- Get experiment's bucketed visits
CREATE OR REPLACE TEMPORARY TABLE xp_visits AS (
  SELECT
    v.visit_id,
    xp.bucketing_id,
    xp.variant_id,
    xp.buyer_segment
  FROM
    `etsy-data-warehouse-prod.weblog.visits` AS v
  INNER JOIN
    first_bucket_segments AS xp
      ON
        xp.bucketing_id = v.browser_id
        AND TIMESTAMP_TRUNC(xp.bucketing_ts, SECOND) <= v.end_datetime
  WHERE
    v._date BETWEEN start_date AND end_date
); */

-- Get browsers who saw the top of the reviews section
CREATE OR REPLACE TEMPORARY TABLE browsers_with_key_event AS (
  SELECT
    v.variant_id,
    v.buyer_segment,
    v.bucketing_id,
    count(case when event_type in ('listing_page_reviews_container_top_seen') then sequence_number end) as top_reviews_events,
    count(case when event_type in ('listing_page_reviews_seen') then sequence_number end) as mid_reviews_events,
  FROM
    first_bucket_segments as v
  LEFT JOIN 
    `etsy-data-warehouse-prod.weblog.events` AS e
      on timestamp_millis(e.epoch_ms) >= v.bucketing_ts-- only look at events that happen after bucketing moment 
      and v.bucketing_id= split(e.visit_id, ".")[0] -- joining on browser_id
      and event_type in ('listing_page_reviews_container_top_seen','listing_page_reviews_seen')
      and e._date BETWEEN start_date AND end_date
  GROUP BY ALL 
);

-- Get KHM aggregated events for experiment's bucketed units
CREATE OR REPLACE TEMPORARY TABLE xp_khm_agg_events AS (
  SELECT
    xp.variant_id,
    xp.buyer_segment,
    xp.top_reviews_events,
    xp.mid_reviews_events,
    xp.bucketing_id,
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
    variant_id,
    buyer_segment,
    top_reviews_events,
    mid_reviews_events,
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

/*
-- Key Health Metrics (Winsorized ACBV and AOV) - Total (To compare with Catapult as a sanity check)
create or replace table etsy-data-warehouse-dev.madelinecollins.xp_feature_tags_desktop as (
SELECT
  xp.variant_id,
  buyer_segment,
  top_reviews_events,
  mid_reviews_events,
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
WHERE 
  lower(buyer_segment) in ('habitual')
GROUP BY
  1
ORDER BY
  1); */
