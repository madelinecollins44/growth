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
   cast(listing_id as string) as listing_id, 
from 
    etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_listing_views 
 );

-------------------------------------------------------------------------------------------
-- Join together events with listings/ visit segments on listing + visit level
-------------------------------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.segments_and_events as (
with events_agg as (
select
  variant_id,
  b.visit_id,
  b.listing_id,
  max(case when purchased_after_view > 0 then 1 else 0 end) as purchased_after_view,
  sum(purchased_after_view) as sum_purchased_after_view,
  -- total counts
  count(case when event_name in ('view_listing') then b.sequence_number end) as listing_views, 
  count(case when event_name in ('listing_page_reviews_seen') then b.sequence_number end) as reviews_seen, 
  count(case when event_name in ('listing_page_reviews_container_top_seen') then b.sequence_number end) as reviews_top_container_seen, 
  count(case when event_name in ('listing_page_review_engagement_frontend') then b.sequence_number end) as listing_page_review_engagements, 
  count(case when event_name in ('listing_page_reviews_pagination') then b.sequence_number end) as paginations, 
  count(case when event_name in ('appreciation_photo_overlay_opened') then b.sequence_number end) as photo_opens, 
  count(case when event_name in ('sort_reviews') then b.sequence_number end) as review_sorts, 
  count(case when event_name in ('reviews_categorical_tag_clicked') then b.sequence_number end) as cat_tag_clicks, 
  count(case when event_name in ('reviews_categorical_tags_seen') then b.sequence_number end) as cat_tags_seen, 
  count(case when event_name in ('listing_page_reviews_content_toggle_opened') then b.sequence_number end) as toggle_opens, 
  -- max counts
  max(case when event_name in ('view_listing') then 1 else 0 end) as viewed_listing, 
  max(case when event_name in ('listing_page_reviews_seen') then 1 else 0 end) as saw_reviews, 
  max(case when event_name in ('listing_page_reviews_container_top_seen') then 1 else 0 end) as has_top_container, 
  max(case when event_name in ('listing_page_review_engagement_frontend') then 1 else 0 end) as has_review_engagement, 
  max(case when event_name in ('listing_page_reviews_pagination') then 1 else 0 end) as has_paginations, 
  max(case when event_name in ('appreciation_photo_overlay_opened') then 1 else 0 end) as has_photo_open, 
  max(case when event_name in ('sort_reviews') then 1 else 0 end) as has_review_sort, 
  max(case when event_name in ('reviews_categorical_tag_clicked') then 1 else 0 end) as has_cat_tag_click, 
  max(case when event_name in ('reviews_categorical_tags_seen') then 1 else 0 end) as has_cat_tag_seen, 
  max(case when event_name in ('listing_page_reviews_content_toggle_opened') then 1 else 0 end) as has_toggle_opens, 
from 
  etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_listing_engagements_agg b
left join 
  etsy-data-warehouse-prod.analytics.listing_views a   
    on cast(a.listing_id as string)=b.listing_id
    and b.visit_id=a.visit_id
    and b.sequence_number=a.sequence_number
    and a._date >= date('2025-06-10')
group by all 
)
, listing_seg as (
select
  coalesce(is_digital,0) as is_digital, 
  coalesce(top_category,'n/a') as top_category,
  coalesce(is_personalizable,0) as is_personalizable,
  coalesce((case when va.listing_id is not null then 1 else 0 end),0) as has_variation,
  coalesce((case 
    when (l.price_usd/100) > 100 then 'high' 
    when (l.price_usd/100) > 30 then 'mid' 
    when (l.price_usd/100) <= 30 then 'low' 
  end),'n/a') as listing_price, -- uses same logic as segment
  coalesce(max(case when r.reviews > 0 or r.listing_id is not null then 1 else 0 end),0) as has_reviews,
  v.listing_id,
from 
  (select distinct listing_id from etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_listing_engagements_agg) v 
left join 
  etsy-data-warehouse-prod.listing_mart.listings l 
    on cast(l.listing_id as string)=v.listing_id 
left join
  etsy-data-warehouse-prod.listing_mart.listing_attributes a 
    on a.listing_id=l.listing_id
left join 
  (select listing_id from etsy-data-warehouse-prod.listing_mart.listing_variations where variation_count > 0) va 
    on va.listing_id=l.listing_id
left join   
  (select listing_id, count(distinct transaction_id) as reviews from etsy-data-warehouse-prod.rollups.transaction_reviews where has_review > 0 group by all ) r   
    on l.listing_id=r.listing_id
group by all 
)
, visit_seg as (
select
  a.visit_id,
  v.platform,
  case when user_id = 0 or user_id is null then 0 else 1 end as signed_in,
  coalesce(new_visitor,0) as new_visitor,
  coalesce(converted,0) as converted,
  coalesce(has_review_engagement,0) as engaged_w_reviews,
from 
  (select 
    visit_id, 
    max(case when event_name in ('listing_page_review_engagement_frontend') then 1 else 0 end) as has_review_engagement 
  from 
    etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_listing_engagements_agg 
  group by all) a
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  _date >= date('2025-06-10')
)
select 
  -- visit segments
  platform,
  signed_in,
  new_visitor,
  converted,
  engaged_w_reviews, 
  -- listing segments 
  is_digital,
  top_category,
  is_personalizable, 
  has_variation,
  listing_price,
  has_reviews,
  -- event metrics 
  e.variant_id,
  e.visit_id,
  e.listing_id,
  -- total counts
  purchased_after_view,
  sum_purchased_after_view,
  listing_views, 
  reviews_seen, 
  reviews_top_container_seen, 
  listing_page_review_engagements, 
  paginations, 
  photo_opens, 
  review_sorts, 
  cat_tag_clicks, 
  cat_tags_seen, 
  toggle_opens, 
  -- max counts
  viewed_listing, 
  saw_reviews, 
  has_top_container, 
  has_review_engagement, 
  has_paginations, 
  has_photo_open, 
  has_review_sort, 
  has_cat_tag_click, 
  has_cat_tag_seen, 
  has_toggle_opens, 
from 
  events_agg e
left join 
  listing_seg l
    on e.listing_id=l.listing_id
left join 
  visit_seg v
    on e.visit_id=v.visit_id
);
