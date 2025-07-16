-------------------------------------------------------------------------------------------
-- Create table to get first bucketing moment of all browsers in the experiment
-------------------------------------------------------------------------------------------
DECLARE config_flag_param STRING DEFAULT "growth_regx.lp_bb_tenure_desktop";
DECLARE start_date DATE; -- DEFAULT "2023-08-22";
DECLARE end_date DATE; -- DEFAULT "2023-09-04";
DECLARE is_event_filtered BOOL; -- DEFAULT FALSE;
DECLARE bucketing_id_type INT64;

IF start_date IS NULL OR end_date IS NULL then
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

IF is_event_filtered IS NULL then
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

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.ab_first_bucket` AS ( -- find the 
select 
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
  experiment_id = config_flag_param
qualify row_number() over (partition by bucketing_id order by bucketing_ts asc) = 1  -- takes the info from the first bucketing moment 
);

-------------------------------------------------------------------------------------------
-- Create table to get all listing views for browsers in the experiment 
-------------------------------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.browsers_w_lv as (
select
	a._date,
  v.variant_id,
	a.visit_id,
  v.bucketing_id, 
	a.sequence_number,
  a.purchased_after_view,
	'view_listing'as event_name,
  listing_id,
  a.seller_user_id,
from
	etsy-data-warehouse-prod.analytics.listing_views a 
inner join 
  etsy-data-warehouse-dev.madelinecollins.ab_first_bucket v
    on split(a.visit_id, ".")[0] = v.bucketing_id  -- only looking at browsers in the experiment 
    and v.bucketing_ts <= timestamp_millis(a.epoch_ms) -- listing views before or after bucketing moment 
where 1=1
  and a._date between date('2025-07-01') and date('2025-07-08') -- dates experiment was live 
group by all 
);

/* create or replace table etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_review_engagements as (
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
  and beacon.event_name in ('listing_page_reviews_seen','listing_page_reviews_container_top_seen','reviews_anchor_click')
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
*/

-------------------------------------------------------------------------------------------
-- Join together events with listings/ visit segments on listing + visit level
-------------------------------------------------------------------------------------------
with trans as (
select
  a.seller_user_id,
  a.listing_id,
  sum(trans_gms_net) as gms_net,
  count(distinct a.transaction_id) as transactions
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans g
inner join  
  etsy-data-warehouse-prod.transaction_mart.all_transactions a using (transaction_id)
where 1=1 
  and a.date between date('2025-07-01') and date('2025-07-08') -- purchases during the experiment 
group by all 
)
, tenure as (
select
  user_id,
  create_date,
  date_diff(current_date(), create_date, month) as months_on_etsy,
    case
      when date_diff(current_date(), create_date, day) < 30 then 'New'
      when date_diff(current_date(), create_date, month) = 1 then '1 month'
      when date_diff(current_date(), create_date, month) between 2 and 11 
        then concat(cast(date_diff(current_date(), create_date, month) as string), ' months')
      when date_diff(current_date(), create_date, month) between 12 and 17 then '1 year'
      when date_diff(current_date(), create_date, month) between 18 and 23 then '1.5 years'
      when date_diff(current_date(), create_date, month) between 24 and 29 then '2 years'
      else concat(cast(round(date_diff(current_date(), create_date, month) / 12.0 * 2) / 2.0 as string), ' years')
  end as tenure_label,
from
  etsy-data-warehouse-prod.rollups.seller_basics
)
, listing_views as (
select
  variant_id,
  bucketing_id,
  seller_user_id,
  listing_id,
  sum(purchased_after_view) as purchases,
  count(distinct visit_id) as visits,
  count(sequence_number) as views,
from 
  etsy-data-warehouse-dev.madelinecollins.browsers_w_lv 
group by all 
)
select
  variant_id,
  tenure_label,
  -- months_on_etsy,
  -- count(distinct bucketing_id) as browsers,
  count(distinct lv.seller_user_id) as shops,
  count(distinct lv.listing_id) as listings,
  sum(purchases) as purchases,
  sum(visits) as visits,
  sum(views) as views,
  sum(gms_net) as total_gms,
  sum(transactions) as total_transactions,
  sum(gms_net)/sum(transactions) as aov,
from 
  listing_views lv
left join 
  tenure te  
    on lv.seller_user_id=te.user_id
left join   
  trans tr 
    on lv.seller_user_id=tr.seller_user_id
    and lv.listing_id=tr.listing_id -- only want to look at listings viewed, not all shop's gms 
group by all 
