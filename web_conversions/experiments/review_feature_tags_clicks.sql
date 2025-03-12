--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- This experiment created feature tags above the review section on the listing page. 
-- This analysis is meant to figure out which tags are getting clicks to inform the team on what kinds of filters would be helpful. 
---- desktop: https://atlas.etsycorp.com/catapult/1356053756481 (growth_regx.lp_review_feature_tags_desktop)
---- mobile web: https://atlas.etsycorp.com/catapult/1356137762600 (growth_regx.lp_review_feature_tags_mweb)
--------------------------------------------------------------------------------------------------------------------------------------------------------------------


--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- DESKTOP
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Define variables
DECLARE config_flag_param STRING DEFAULT " growth_regx.lp_review_feature_tags_desktop";
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

-- Get feature tag related events for all bucketed visits 
CREATE OR REPLACE TEMPORARY TABLE tag_events AS (
select
  visit_id,
  sequence_number,
  beacon.event_name as event_name,
  (select value from unnest(beacon.properties.key_value) where key = "tag_type") as tag_type
from 
  etsy-visit-pipe-prod.canonical.visit_id_beacons b
inner join 
  xp_visits v using (visit_id)
where
  date(b._partitiontime) >= current_date-30
  and beacon.event_name in ('reviews_feature_tags_seen','reviews_feature_tag_clicked') 
);

-- How many clicks did each type of tag get? 
select
  tag_type,
  count(sequence_number) as clicks
from 
  tag_events
where
  event_name ('reviews_feature_tag_clicked')
group by all 

--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- MOBILE WEB 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Define variables
DECLARE config_flag_param STRING DEFAULT "growth_regx.lp_review_feature_tags_mweb";
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

