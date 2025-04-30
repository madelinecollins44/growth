/*

Note: This script is intended to analyze custom date ranges for Catapult experiments
and thus relies on more "raw" forms of experiment data.
If you are not interested in custom date ranges (i.e. you want to analyze the experiment boundaries
as defined in Catapult), please use the catapult_unified_event_level_data_simplified.sql instead,
which uses pre-aggregated data that allows the script to run faster.

This script contains queries on the catapult_unified tables that can be used for analyzing experiment results.
It takes in an experiment (config flag) and date range as input, and provides the following data:
* First bucketing moment for each experimental unit (i.e. browser or user)
* Segment values for each experimental unit
* Event counts, GMS, and visit count for each experimental unit
* Combined data for all events and experimental units, with segment values
* Query to recreate the metric results from the Catapult UI

If you want to join catapult data with non-catapult tables that rely on visit ids,
this script also includes example queries on how to get visit ids for your experiment.

For more details on how to work with catapult_unified tables, go to go/catapult-unified-docs
*/

-------------------------------------------------------------------------------------------
-- INPUT
-------------------------------------------------------------------------------------------
DECLARE config_flag_param STRING DEFAULT "growth_regx.lp_review_categorical_tags_mweb";

-- By default, this script uses the latest experiment boundary dates for the given experiment.
-- If you want to specify a custom date range, you can also specify the start and end date manually.
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
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` AS (
    SELECT
        bucketing_id,
        bucketing_id_type AS bucketing_id_type,
        variant_id,
        MIN(bucketing_ts) AS bucketing_ts,
    FROM
        `etsy-data-warehouse-prod.catapult_unified.bucketing`
    WHERE
        _date BETWEEN start_date AND end_date
        AND experiment_id = config_flag_param
    GROUP BY
        bucketing_id, bucketing_id_type, variant_id
);

-- For event filtered experiments, the effective bucketing event for a bucketed unit
-- into a variant is the FIRST filtering event to occur after that bucketed unit was
-- bucketed into that variant of the experiment.
    
    

--     CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` AS (
--         SELECT
--             a.bucketing_id,
--             a.bucketing_id_type,
--             a.variant_id,
--         MIN(bucketing_ts) AS bucketing_ts,
-- FROM
--             `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` a
--         INNER JOIN 
--             etsy-data-warehouse-prod.weblog.visits b
--                 on b.browser_id=a.bucketing_id
--         INNER JOIN 
--               etsy-data-warehouse-prod.weblog.events c
--                 on b.visit_id=c.visit_id
--         -- JOIN
--         --     `etsy-data-warehouse-prod.catapult_unified.filtering_event` f
--         --     USING(bucketing_id)
--         WHERE
--             event_type in ('reviews_categorical_tag_clicked')
--             and b._date >= current_date-30
--             and timestamp_millis(epoch_ms) >= a.bucketing_ts-- only look at 
--             -- AND f.experiment_id = config_flag_param
--             -- AND f.event_ts >= f.boundary_start_ts
--             -- AND f.event_ts >= a.bucketing_ts
--         GROUP BY
--             bucketing_id, bucketing_id_type, variant_id
--     );

-------------------------------------------------------------------------------------------
-- SEGMENT DATA
-------------------------------------------------------------------------------------------
-- Get segment values based on first bucketing moment.
-- Example output:
-- bucketing_id | variant_id | event_id         | event_value
-- 123          | off        | buyer_segment    | New
-- 123          | off        | canonical_region | FR
-- 456          | on         | buyer_segment    | Habitual
-- 456          | on         | canonical_region | US
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.first_bucket_segments_unpivoted` AS (
    SELECT
        a.bucketing_id,
        a.variant_id,
        s.event_id,
        s.event_value,
    FROM
        `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` a
    JOIN
        `etsy-data-warehouse-prod.catapult_unified.segment_event_custom` s
        USING(bucketing_id, bucketing_ts)
    WHERE
        s._date BETWEEN start_date AND end_date
        AND s.experiment_id = config_flag_param
        -- <SEGMENTATION> Here you can specify whatever segmentations you'd like to analyze.
        -- !!! Please keep this in sync with the PIVOT statement below !!!
        -- For all supported segmentations, see go/catapult-unified-docs.
        AND s.event_id IN (
            "buyer_segment",
            "new_visitor"
        )
);

-- Pivot the above table to get one row per bucketing_id and variant_id. Each additional
-- column will be a different segmentation, and the value will be the segment for each
-- bucketing_id at the time they were first bucketed into the experiment date range being
-- analyzed.
-- Example output (using the same example data above):
-- bucketing_id | variant_id | buyer_segment | canonical_region
-- 123          | off        | New           | FR
-- 456          | on         | Habitual      | US
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.first_bucket_segments` AS (
    SELECT
        *
    FROM
        `etsy-data-warehouse-dev.madelinecollins.first_bucket_segments_unpivoted`
    PIVOT(
        MAX(event_value)
        FOR event_id IN (
            "buyer_segment",
            "new_visitor"
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
            "bounce",
            "checkout_start",
            "backend_add_to_cart",
            "total_winsorized_order_value"
        ]) AS event_id
);

-- Get all the bucketed units with the events of interest.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.events_per_unit` AS (
    SELECT
        a.bucketing_id,
        a.variant_id,
        e.event_id,
        CAST(SUM(e.event_value) AS FLOAT64) AS event_value,
    FROM
        `etsy-data-warehouse-prod.catapult_unified.event` e
    CROSS JOIN
        UNNEST(e.associated_ids) ids
    JOIN
        `etsy-data-warehouse-dev.madelinecollins.events`
        USING(event_id)
    JOIN
        `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` a
        ON a.bucketing_id = ids.id
        AND a.bucketing_id_type = ids.id_type
    WHERE
        e._date BETWEEN start_date AND end_date
        AND e.event_type IN (1, 3, 4) -- fired, gms, and bounce events (see go/catapult-unified-enums)
        AND e.event_ts >= a.bucketing_ts
    GROUP BY
        bucketing_id, variant_id, event_id
);

-- Insert custom events separately, as custom event data does not exist in the event table (as of Q4 2023).
IF bucketing_id_type = 1 THEN -- browser data (see go/catapult-unified-enums)
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.post_bucketing_custom_events` AS (
        WITH custom_events AS (
            SELECT
                a.bucketing_id,
                v.visit_id,
                a.variant_id,
                a.bucketing_ts,
                v.sequence_number,
                v.event_name AS event_id,
                v.event_data AS event_value,
                v.event_timestamp,
            FROM
                `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` a
            JOIN
                `etsy-data-warehouse-prod.catapult.visit_segment_custom_metrics` v
                ON a.bucketing_id = SPLIT(v.visit_id, '.')[OFFSET(0)]
            WHERE
                v._date BETWEEN start_date AND end_date
                AND v.event_timestamp >= a.bucketing_ts
        )
        SELECT
            bucketing_id,
            visit_id,
            variant_id,
            bucketing_ts,
            sequence_number,
            event_id,
            event_value,
            ROW_NUMBER() OVER (
                PARTITION BY bucketing_id, variant_id, event_id
                ORDER BY event_timestamp, visit_id, sequence_number
            ) AS row_number,
        FROM
            custom_events
    );

    INSERT INTO `etsy-data-warehouse-dev.madelinecollins.events_per_unit` (
        SELECT
            bucketing_id,
            variant_id,
            event_id,
            SUM(event_value) AS event_value,
        FROM
            `etsy-data-warehouse-dev.madelinecollins.post_bucketing_custom_events`
        WHERE
            row_number = 1
            OR (row_number > 1 AND sequence_number = 0)
        GROUP BY
            bucketing_id, variant_id, event_id
    );
ELSEIF bucketing_id_type = 2 THEN -- user data (see go/catapult-unified-enums)
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.post_bucketing_custom_events` AS (
        WITH custom_events AS (
            SELECT
                a.bucketing_id,
                c.visit_id,
                a.variant_id,
                a.bucketing_ts,
                c.sequence_number,
                c.event_name AS event_id,
                c.event_data AS event_value,
                c.event_timestamp,
            FROM
                `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` a
            JOIN
                `etsy-data-warehouse-prod.catapult.custom_events_by_user_slice` c
                ON a.bucketing_id = c.user_id
            WHERE
                c._date BETWEEN start_date AND end_date
                AND c.event_timestamp >= a.bucketing_ts
        )
        SELECT
            bucketing_id,
            visit_id,
            variant_id,
            bucketing_ts,
            sequence_number,
            event_id,
            event_value,
            ROW_NUMBER() OVER (
                PARTITION BY bucketing_id, variant_id, event_id
                ORDER BY event_timestamp, visit_id, sequence_number
            ) AS row_number,
            ROW_NUMBER() OVER (
                PARTITION BY bucketing_id, variant_id, event_id, visit_id
                ORDER BY sequence_number
            ) AS row_number_in_visit,
        FROM
            custom_events
    );

    INSERT INTO `etsy-data-warehouse-dev.madelinecollins.events_per_unit` (
        SELECT
            bucketing_id,
            variant_id,
            event_id,
            SUM(event_value) AS event_value,
        FROM
            `etsy-data-warehouse-dev.madelinecollins.post_bucketing_custom_events`
        WHERE
            row_number = 1
            OR (row_number > 1 AND row_number_in_visit = 1)
        GROUP BY
            bucketing_id, variant_id, event_id
    );
END IF;

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
        event_id,
        COALESCE(event_value, 0) AS event_count,
        case when LOWER(buyer_segment) in ('signed_out') then 'signed_out' else 'signed_in' end as buyer_segment,
        new_visitor,
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
    event_id,
    variant_id,
    -- buyer_segment,
    count(distinct bucketing_id) as unique_browsers,
    COUNT(*) AS total_units_in_variant,
    AVG(IF(event_count = 0, 0, 1)) AS percent_units_with_event,
    AVG(event_count) AS avg_events_per_unit,
    AVG(IF(event_count = 0, NULL, event_count)) AS avg_events_per_unit_with_event
FROM
    `etsy-data-warehouse-dev.madelinecollins.all_units_events_segments`
GROUP BY ALL
ORDER BY event_id, variant_id;

-------------------------------------------------------------------------------------------
-- VISIT IDS TO JOIN WITH EXTERNAL TABLES
-------------------------------------------------------------------------------------------
-- Need visit ids to join with non-Catapult tables?
-- No problem! Here are some examples for how to get the visit ids for each experimental unit.

-- All associated IDs in the bucketing visit
IF NOT is_event_filtered THEN
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` AS (
        SELECT
            a.bucketing_id,
            a.bucketing_id_type,
            a.variant_id,
            a.bucketing_ts,
            (SELECT id FROM UNNEST(b.associated_ids) WHERE id_type = 4) AS sequence_number,
            (SELECT id FROM UNNEST(b.associated_ids) WHERE id_type = 1) AS browser_id,
            (SELECT id FROM UNNEST(b.associated_ids) WHERE id_type = 2) AS user_id,
            (SELECT id FROM UNNEST(b.associated_ids) WHERE id_type = 3) AS visit_id,
        FROM
            `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` a
        JOIN
            `etsy-data-warehouse-prod.catapult_unified.bucketing` b
            USING(bucketing_id, variant_id, bucketing_ts)
        WHERE
            b._date BETWEEN start_date AND end_date
            AND b.experiment_id = config_flag_param
    );
ELSE
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` AS (
        SELECT
            a.bucketing_id,
            a.bucketing_id_type,
            a.variant_id,
            a.bucketing_ts,
            (SELECT id FROM UNNEST(f.associated_ids) WHERE id_type = 4) AS sequence_number,
            (SELECT id FROM UNNEST(f.associated_ids) WHERE id_type = 1) AS browser_id,
            (SELECT id FROM UNNEST(f.associated_ids) WHERE id_type = 2) AS user_id,
            (SELECT id FROM UNNEST(f.associated_ids) WHERE id_type = 3) AS visit_id,
        FROM
            `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` a
        JOIN
            `etsy-data-warehouse-prod.catapult_unified.filtering_event` f
            ON a.bucketing_id = f.bucketing_id
            AND a.bucketing_ts = f.event_ts
        WHERE
            f._date BETWEEN start_date AND end_date
            AND f.experiment_id = config_flag_param
    );
END IF;

-------------------------------------------------------------------------------------------
-- RECREATE CATAPULT RESULTS : browser level so can find stat sig of means 
-------------------------------------------------------------------------------------------
-- -- Proportion and mean metrics by variant and event_name
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level` AS (
SELECT
    event_id,
    variant_id,
    bucketing_id, 
    event_count,
FROM
    `etsy-data-warehouse-dev.madelinecollins.all_units_events_segments_included`
GROUP BY
    all
ORDER BY
    event_id, variant_id
);

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_acbv` AS (
  select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level` where event_id in ('total_winsorized_gms')
);

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_order_value` AS (
  select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level` where event_id in ('total_winsorized_order_value')
);

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_conversion` AS (
  select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level` where event_id in ('backend_cart_payment')
);
