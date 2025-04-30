--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- DESKTOP
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Define variables
DECLARE config_flag_param STRING DEFAULT "growth_regx.lp_review_categorical_tags_mweb";
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

CREATE OR REPLACE TEMPORARY TABLE xp_units_raw AS (
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

-- Get experiment's bucketed units
CREATE OR REPLACE TEMPORARY TABLE xp_units AS (
  SELECT 
    bucketing_id,
    variant_id,
    bucketing_ts,
    v.visit_id,
    listing_id,
  FROM
    `etsy-data-warehouse-prod.catapult_unified.bucketing_period` xp
  INNER JOIN 
    `etsy-data-warehouse-prod.weblog.visits` AS v
   ON
      xp.bucketing_id = v.browser_id
      AND TIMESTAMP_TRUNC(xp.bucketing_ts, SECOND) <= v.end_datetime
  INNER JOIN 
    `etsy-data-warehouse-prod.weblog.events` AS e
      on e.visit_id = v.visit_id
      AND TIMESTAMP_TRUNC(xp.bucketing_ts, SECOND) = v.end_datetime
  WHERE
    xp._date = end_date
    AND experiment_id = config_flag_param
    AND  v._date BETWEEN start_date AND end_date
);

/* HERE, RECREATE METRICS IN CATAPULT USING EVENT FILTER */
-- Get browsers who saw the listing grid
CREATE OR REPLACE TEMPORARY TABLE browsers_with_key_event AS (
  SELECT DISTINCT
    v.bucketing_id
  FROM
    `etsy-data-warehouse-prod.weblog.events` AS e
  INNER JOIN 
    xp_units AS v 
        ON v.bucketing_id=split(e.visit_id,'.')[safe_offset(0)] -- this is how to get browser_id from visit
  WHERE
    e._date BETWEEN start_date AND end_date
    AND e.event_type = "reviews_categorical_tag_clicked" -- event fires when a browser sees the listing grid 
);

-- GET AVG RATING FOR LISTING
CREATE OR REPLACE TEMPORARY TABLE listing_rating AS (
SELECT
    listing_id,
    case 
      when coalesce(avg(rating),0) = 0 then '0'
      when coalesce(avg(rating),0) > 0 and coalesce(avg(rating),0) <= 1 then '1'
      when coalesce(avg(rating),0) > 1 and coalesce(avg(rating),0)<= 2 then '2'
      when coalesce(avg(rating),0) > 2 and coalesce(avg(rating),0)<= 3 then '3'
      when coalesce(avg(rating),0) > 3 and coalesce(avg(rating),0) <= 4 then '4'
      when coalesce(avg(rating),0) > 4 and coalesce(avg(rating),0)<= 5 then '5'
      else 'error'
      end as avg_rating
  FROM
    `etsy-data-warehouse-prod.rollups.transaction_reviews` 
  group by all 
);

select
  avg_rating,
  count(distinct u.bucketing_id) as bucketed_units,
  count(distinct ke.bucketing_id) as bucketed_units_to_click,
from 
  xp_units u
left join 
  browsers_with_key_event ke
    on u.bucketing_id=ke.bucketing_id
left join 
  listing_rating lr 
    on lr.listing_id=u.listing_id
group by all 
