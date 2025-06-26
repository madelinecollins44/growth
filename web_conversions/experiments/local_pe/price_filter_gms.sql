
-------------------------------------------------------------------------------------------
-- INPUT
-------------------------------------------------------------------------------------------
DECLARE config_flag_param STRING DEFAULT "growth_regx.sh_add_price_filters_desktop";
DECLARE start_date DATE; -- DEFAULT "2023-08-22";
DECLARE end_date DATE; -- DEFAULT "2023-09-04";
DECLARE is_event_filtered BOOL; -- DEFAULT FALSE;
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

--ADD IN EVENT FILTER
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` AS (
        SELECT
            max(case when event_type in ('shop_home_filter_dropdown_open') then 1 else 0 end) as 
            a.bucketing_id,
            b.visit_id,   
            b.converted,
            b.total_gms,
            a.variant_id,
            MIN(timestamp_millis(c.epoch_ms)) AS bucketing_ts,
            -- MIN(f.event_ts) AS bucketing_ts,
        FROM
            `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` a
       INNER JOIN 
            etsy-data-warehouse-prod.weblog.visits b
                on b.browser_id=a.bucketing_id
        INNER JOIN 
              etsy-data-warehouse-prod.weblog.events c
                on b.visit_id=c.visit_id
        -- JOIN
        --     `etsy-data-warehouse-prod.catapult_unified.filtering_event` f
        --     USING(bucketing_id)
        WHERE 1=1
            and b._date >= date('04-22-2025') -- start of PE
            and timestamp_millis(epoch_ms) >= a.bucketing_ts-- only look at 
            -- AND f.experiment_id = config_flag_param
            -- AND f.event_ts >= f.boundary_start_ts
            -- AND f.event_ts >= a.bucketing_ts 
        GROUP BY
            ALL
    );
