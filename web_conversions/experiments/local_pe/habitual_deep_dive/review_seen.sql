-----------------------------------------------------------------------------
-- CREATE BASE TABLES FOR HABITUALS BROWSERS IN PE
-----------------------------------------------------------------------------
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

