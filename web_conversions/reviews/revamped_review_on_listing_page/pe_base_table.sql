------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- This table aggregates all the browsers in the local buyer trust pe. it includes every visit after the bucketing moment, as well as the bucketing time stamp.
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
DECLARE config_flag_param STRING DEFAULT "local_pe.q2_2025.buyer_trust_accelerator.browser";
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
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` AS ( -- find the 
    SELECT
        bucketing_id,
        bucketing_id_type,
        variant_id,
        date(bucketing_ts) as bucketing_date,
        IF(is_event_filtered, filtered_bucketing_ts, bucketing_ts) AS bucketing_ts,
    FROM
        `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
    WHERE
        _date = end_date
        AND experiment_id = config_flag_param
        AND ((NOT is_event_filtered) OR (filtered_bucketing_ts IS NOT NULL))
);

-------------------------------------------------------------------------------------------
-- GET ALL VISITS FOR BROWSERS BUCKETED INTO THE PE POST CAT TAGS BEING RAMPED UP 
-------------------------------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_post_cattags as (
select
  f.bucketing_date,
  f.bucketing_ts, -- this is the bucketing timestamp, use this to look at all events AFTER the bucketing timestamp 
  f.variant_id,
  f.bucketing_id,
  v.browser_id,
  v.visit_id,
  v._date as visit_date
from 
  etsy-data-warehouse-dev.madelinecollins.ab_first_bucket f
inner join 
  etsy-data-warehouse-prod.weblog.visits v
    on f.bucketing_id=v.browser_id -- browsers in the pe 
    and f.bucketing_ts <= v.start_datetime -- all visits after the initial bucketed visit
where 1=1
  and f.bucketing_date >= date('2025-06-10') -- two weeks after the cat tag experiment was ramped up 
  and v._date >=  date('2025-06-10') -- two weeks after the cat tag experiment was ramped up 
);

-------------------------------------------------------------------------------------------
-- Pull all listing events for a browser post bucketing
-------------------------------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_listing_engagements as (
select
	date(_partitiontime) as _date,
  v.variant_id,
	v.visit_id,
	v.sequence_number,
	beacon.event_name as event_name,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` vb
inner join 
  etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_post_cattags v -- only looking at browsers in the experiment 
    on v.bucketing_ts <= vb.beacon.timestamp -- join to the browser table for everything AFTER the browser was bucketed into the PE  
where 1=1
  and beacon.event_name in ('view_listing','listing_page_reviews_seen','listing_page_reviews_container_top_seen','listing_page_review_engagement_frontend','listing_page_reviews_pagination','appreciation_photo_overlay_opened','sort_reviews','reviews_categorical_tag_clicked','reviews_categorical_tags_seen','listing_page_reviews_content_toggle_opened')
  group by all 
);
