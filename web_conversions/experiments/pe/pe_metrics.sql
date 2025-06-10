DECLARE config_flag_param STRING DEFAULT "local_pe.q2_2025.buyer_trust_accelerator.browser";

-- By default, this script uses the latest experiment boundary dates for the given experiment.
-- If you want to specify an earlier experiment boundary, you can do so by specifying the start and end date manually.
DECLARE start_date DATE; -- DEFAULT "2023-08-22";
DECLARE end_date DATE; -- DEFAULT "2023-09-04";

-- By default, this script automatically detects whether the experiment is event filtered or not
-- and provides the associated analysis. However, in the case that we want to examine non-filtered
-- results for an event filtered experiment, this variable may be manually set to "FALSE".
DECLARE is_event_filtered BOOL; -- DEFAULT FALSE;

-- Generally, this variable should not be overridden, as the grain of analysis should match the
-- bucketing ID type.
DECLARE bucketing_id_type INT64;

IF start_date IS NULL OR end_date IS NULL THEN
    SET (start_date, end_date) = (
        SELECT AS STRUCT
            MAX(DATE(boundary_start_ts)) AS start_date,
            MAX(_date) AS end_date,
        FROM
            `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
            experiment_id = config_flag_param
    );
END IF;

IF is_event_filtered IS NULL THEN
    SET (is_event_filtered, bucketing_id_type) = (
        SELECT AS STRUCT
            is_filtered,
            bucketing_id_type,
        FROM
            `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
            _date = end_date
            AND experiment_id = config_flag_param
    );
ELSE
    SET bucketing_id_type = (
        SELECT
            bucketing_id_type,
        FROM
            `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
            _date = end_date
            AND experiment_id = config_flag_param
    );
END IF;

-- TIPS:
--   - Replace 'madelinecollins' in the table names below with your own username or personal dataset name.
--   - Additionally, there are a few TODO items in this script depending on:
--       - Whether you would like to look at certain segmentations  (marked with <SEGMENTATION>)
--       - Whether you would like to look at certain events         (marked with <EVENT>)
--     Before running, please review the script and adjust the marked sections accordingly!

-------------------------------------------------------------------------------------------
-- BUCKETING DATA
-------------------------------------------------------------------------------------------
-- Get the first bucketing moment for each experimental unit (e.g. browser or user).
-- If is_event_filtered is true, then only select experimental unit whose `filtered_bucketing_ts` is defined.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` AS (
    SELECT
        bucketing_id,
        bucketing_id_type,
        variant_id,
        date(bucketing_ts) as bucketing_date
        IF(is_event_filtered, filtered_bucketing_ts, bucketing_ts) AS bucketing_ts,
    FROM
        `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
    WHERE
        _date = end_date
        AND experiment_id = config_flag_param
        AND ((NOT is_event_filtered) OR (filtered_bucketing_ts IS NOT NULL))
);

-------------------------------------------------------------------------------------------
-- SEGMENT DATA
-------------------------------------------------------------------------------------------
-- For each bucketing_id and variant_id, output one row with their segment assignments.
-- Each additional column will be a different segmentation, and the value will be the segment for each
-- bucketing_id at the time they were first bucketed into the experiment date range being
-- analyzed.
-- Example output (using the same example data above):
-- bucketing_id | variant_id | buyer_segment | canonical_region
-- 123          | off        | New           | FR
-- 456          | on         | Habitual      | US
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.first_bucket_segments` AS (
    WITH first_bucket_segments_unpivoted AS (
        SELECT
            bucketing_id,
            variant_id,
            event_id,
            IF(is_event_filtered, filtered_event_value, event_value) AS event_value
        FROM
            `etsy-data-warehouse-prod.catapult_unified.aggregated_segment_event`
        WHERE
            _date = end_date
            AND experiment_id = config_flag_param
            -- <SEGMENTATION> Here you can specify whatever segmentations you'd like to analyze.
            -- !!! Please keep this in sync with the PIVOT statement below !!!
            -- For all supported segmentations, see go/catapult-unified-docs.
            AND event_id IN (
                "buyer_segment",
                "canonical_region"
            )
            AND ((NOT is_event_filtered) OR (filtered_bucketing_ts IS NOT NULL))
    )
    SELECT
        *
    FROM
        first_bucket_segments_unpivoted
    PIVOT(
        MAX(event_value)
        FOR event_id IN (
            "buyer_segment",
            "canonical_region"
        )
    )
);

-------------------------------------------------------------------------------------------
-- EVENT AND GMS DATA
-------------------------------------------------------------------------------------------
-- <EVENT> Specify the events you want to analyze here.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.events` AS (
    SELECT
        *
    FROM
        UNNEST([
            "backend_cart_payment", -- conversion rate
            "total_winsorized_gms", -- winsorized acbv
            "prolist_total_spend",  -- prolist revenue
            "gms"                   -- note: gms data is in cents
        ]) AS event_id
);

-- Get all the bucketed units with the events of interest.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.events_per_unit` AS (
    SELECT
        bucketing_id,
        variant_id,
        event_id,
        IF(is_event_filtered, filtered_event_value, event_value) AS event_value
    FROM
        `etsy-data-warehouse-prod.catapult_unified.aggregated_event_func`(start_date, end_date)
    JOIN
        `etsy-data-warehouse-dev.madelinecollins.events`
        USING (event_id)
    WHERE
        experiment_id = config_flag_param
        AND ((NOT is_event_filtered) OR (filtered_bucketing_ts IS NOT NULL))
);

-------------------------------------------------------------------------------------------
-- VISIT COUNT
-------------------------------------------------------------------------------------------

-- Get all post-bucketing visits for each experimental unit
IF bucketing_id_type = 1 THEN -- browser data (see go/catapult-unified-enums)
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.subsequent_visits` AS (
        SELECT
            b.bucketing_id,
            b.variant_id,
            v.visit_id,
        FROM
            `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` b
        JOIN
            `etsy-data-warehouse-prod.weblog.visits` v
            ON b.bucketing_id = v.browser_id
            AND TIMESTAMP_TRUNC(bucketing_ts, SECOND) <= v.end_datetime
        WHERE
            v._date BETWEEN start_date AND end_date
    );
ELSEIF bucketing_id_type = 2 THEN -- user data (see go/catapult-unified-enums)
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.subsequent_visits` AS (
        SELECT
            b.bucketing_id,
            b.variant_id,
            v.visit_id,
        FROM
            `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` b
        JOIN
            `etsy-data-warehouse-prod.weblog.visits` v
            -- Note that for user experiments, you may miss out on some visits in cases where multiple
            -- users share the same visit_id. This is because only the first user_id is recorded in
            -- the weblog.visits table (as of Q4 2023).
            --
            -- Additionally, the only difference between the user and browser case is the join on
            -- bucketing_id. However, due to performance reasons, we apply our conditional logic at
            -- a higher level rather than in the join itself.
            ON b.bucketing_id = CAST(v.user_id AS STRING)
            AND TIMESTAMP_TRUNC(bucketing_ts, SECOND) <= v.end_datetime
        WHERE
            v._date BETWEEN start_date AND end_date
    );
END IF;

-- Get visit count per experimental unit
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.visits_per_unit` AS (
    SELECT
        bucketing_id,
        variant_id,
        COUNT(*) AS visit_count,
    FROM
        `etsy-data-warehouse-dev.madelinecollins.subsequent_visits`
    GROUP BY
        bucketing_id, variant_id
);

-------------------------------------------------------------------------------------------
-- COMBINE BUCKETING, EVENT & SEGMENT DATA
-------------------------------------------------------------------------------------------
-- All events for all bucketed units, with segment values.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.all_units_events_segments` AS (
    SELECT
        bucketing_id,
        variant_id,
        bucketing_date,
        event_id,
        COALESCE(event_value, 0) AS event_count,
        buyer_segment,
        canonical_region,
    FROM
        `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket`
    CROSS JOIN
        `etsy-data-warehouse-dev.madelinecollins.events`
    LEFT JOIN
        `etsy-data-warehouse-dev.madelinecollins.events_per_unit`
        USING(bucketing_id, variant_id, event_id)
    JOIN
        `etsy-data-warehouse-dev.madelinecollins.first_bucket_segments`
        USING(bucketing_id, variant_id)
);

-------------------------------------------------------------------------------------------
-- RECREATE CATAPULT RESULTS
-------------------------------------------------------------------------------------------
-- Proportion and mean metrics by variant and event_name
SELECT
    -- bucketing_date,
    variant_id,
    COUNT(distinct bucketing_id) AS total_units_in_variant,
    avg(case when event_id in ('backend_cart_payment') then IF(event_count = 0, 0, 1) end) AS conversion_rate,
    (sum(case when event_id in ('gms') then event_count end)/100) AS total_gms,
    (sum(case when event_id in ('gms') then event_count end)/100)/ COUNT(distinct bucketing_id) AS gms_per_unit,
    avg(case when event_id in ('total_winsorized_gms') then IF(event_count = 0, NULL, event_count) end) AS acbv
FROM
    `etsy-data-warehouse-dev.madelinecollins.all_units_events_segments`
GROUP BY
    variant_id
