-------------------------------------------------------------------------------------------
-- Create table to get first bucketing moment of all browsers in the PE
-------------------------------------------------------------------------------------------
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

-- Get the first bucketing moment for each experimental unit (e.g. browser or user).
-- If is_event_filtered is true, then only select experimental unit whose `filtered_bucketing_ts` is defined.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` AS ( -- find the 
with agg as (
sELECT 
  _date,
  variant_id,
  bucketing_ts, 
  bucketing_id,
  experiment_id,
  (select id from unnest(associated_ids) where id_type = 3) as visit_id,
  (select cast(id as int) from unnest(associated_ids) where id_type = 4) as sequence_number
from
  `etsy-data-warehouse-prod.catapult_unified.bucketing`
where
  experiment_id = 'local_pe.q2_2025.buyer_trust_accelerator.browser'
qualify row_number() over (partition by bucketing_id order by bucketing_ts asc) = 1  -- takes the info from the first bucketing moment 
)
select * from agg where _date >= date('2025-06-10') 
);

-------------------------------------------------------------------------------------------
-- Create table to get all listing engagements for browsers in the pe 
-------------------------------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_listing_views as (
select
	a._date,
    v.variant_id,
	a.visit_id,
    v.bucketing_id, 
	a.sequence_number,
	'view_listing'as event_name,
    listing_id,
from
	etsy-data-warehouse-prod.analytics.listing_views a 
inner join 
  etsy-data-warehouse-dev.madelinecollins.ab_first_bucket v -- only looking at browsers in the experiment 
    on split(a.visit_id, ".")[0] = v.bucketing_id 
    and v.bucketing_ts <= timestamp_millis(a.epoch_ms) -- listing views before or after bucketing moment 
    -- and ((vb.visit_id = v.visit_id AND vb.sequence_number >= v.sequence_number) -- in the bucketing visit, anything after the bucketing sequence number 
    --        or (vb.visit_id > v.visit_id)) -- everything after the bucketing visit 
where 1=1
  and a._date >= date('2025-06-10') 
group by all 
);

create or replace table etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_review_engagements as (
select
	date(_partitiontime) as _date,
    v.variant_id,
	vb.visit_id,
    v.bucketing_id,
	vb.sequence_number,
	beacon.event_name as event_name,
    regexp_extract(beacon.loc, r'listing/(\d+)') as listing_id 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` vb
inner join 
  etsy-data-warehouse-dev.madelinecollins.ab_first_bucket v -- only looking at browsers in the experiment 
    on split(vb.visit_id, ".")[0] = v.bucketing_id 
    and v.bucketing_ts <= timestamp_millis(vb.beacon.timestamp) 
    -- and ((vb.visit_id = v.visit_id AND vb.sequence_number >= v.sequence_number) -- in the bucketing visit, anything after the bucketing sequence number 
    --        or (vb.visit_id > v.visit_id)) -- everything after the bucketing visit 
where 1=1
  and date(_partitiontime) >= date('2025-06-10') 
  and beacon.event_name in ('listing_page_reviews_seen','listing_page_reviews_container_top_seen','listing_page_review_engagement_frontend','listing_page_reviews_pagination','appreciation_photo_overlay_opened','sort_reviews','reviews_categorical_tag_clicked','reviews_categorical_tags_seen','listing_page_reviews_content_toggle_opened')
  group by all 
);
 
 -- create agg table to look at all listing engagements 
 create or replace table etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_listing_engagements_agg as (
 select
 	_date,
    variant_id,
	visit_id,
    bucketing_id,
	sequence_number,
	event_name,
    listing_id 
from 
    etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_review_engagements 
union all 
 select
 	_date,
    variant_id,
	visit_id,
    bucketing_id,
	sequence_number,
	event_name,
    listing_id 
from 
    etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_listing_views 
 );
