DECLARE config_flag_param STRING DEFAULT "growth_regx.lp_move_appreciation_photos_mweb";
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
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.xp_units_temp` AS (
  WITH
    xp_units AS (
      SELECT 
        bucketing_id,
        variant_id,
        bucketing_ts
      FROM
        `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
      WHERE
        _date = end_date
        AND experiment_id = config_flag_param
    ),

    xp_units_with_bucketing_visit_id AS (
      SELECT
        xp.*,
        (SELECT id FROM UNNEST(b.associated_ids) WHERE id_type = 3) AS bucketing_visit_id,
        (SELECT CAST(id AS INT64) FROM UNNEST(b.associated_ids) WHERE id_type = 4) AS bucketing_sequence_number
      FROM
        xp_units AS xp
      LEFT JOIN
        `etsy-data-warehouse-prod.catapult_unified.bucketing` AS b
          ON
            b.bucketing_id = xp.bucketing_id
            AND b.bucketing_ts = xp.bucketing_ts
            AND b._date BETWEEN start_date AND end_date
            AND b.experiment_id = config_flag_param
    )

  SELECT
    *
  FROM
    xp_units_with_bucketing_visit_id

);

-- Get % of bucketed units that were bucketed in a listing view
WITH
  events AS (
    SELECT
      visit_id,
      event_type,
      sequence_number
    FROM
      etsy-data-warehouse-prod.weblog.events 
    WHERE
      _date BETWEEN start_date AND end_date
  )
    SELECT
      xp.variant_id,
      event_type,
      COUNT(bucketing_id) AS total_units,
      -- COUNTIF(lv.visit_id IS NOT NULL)/COUNT(bucketing_id) AS units_bucketed_in_lv_pct
    FROM
      `etsy-data-warehouse-dev.madelinecollins.xp_units_temp` AS xp
    LEFT JOIN
      events AS lv
        ON
          lv.visit_id = xp.bucketing_visit_id
          AND lv.sequence_number = xp.bucketing_sequence_number
    GROUP BY all
    ORDER BY
      1;
