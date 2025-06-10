begin
create or replace temp table bucketing_listing_ids as (
with bucketing as ( -- pulls out bucketing moment 
SELECT 
  _date,
  bucketing_ts, 
  bucketing_id,
  experiment_id,
  associated_ids
from
  `etsy-data-warehouse-prod.catapult_unified.bucketing`
inner join 
  (SELECT distinct bucketing_id
  FROM
    `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
  WHERE _date is not null AND experiment_id = 'growth_regx.lp_move_appreciation_photos_mweb')
  using (bucketing_id)
) 
, indexesOfInterest as (
  select b.*,
  e.bucketing_id_type as bucketing_id_type,
  (SELECT id FROM UNNEST(associated_ids) WHERE id_type = 3) as visit_id,
  (SELECT id FROM UNNEST(associated_ids) WHERE id_type = 2) as user_id,
  (SELECT id FROM UNNEST(associated_ids) WHERE id_type = 1) as browser_id,
   (select cast(id as int) from unnest(associated_ids) where id_type = 4) as sequence_number
from 
  bucketing b
join 
  `etsy-data-warehouse-prod.catapult_unified.experiment` e using (experiment_id)
where 
  e._date is not null 
)
, ListingID as (
SELECT 
  (select value from unnest(beacon.properties.key_value) where key = "listing_id") as listing_id, 
  beacon.browser_id as browser_id, 
  cast(beacon.user_id as string) as user_id, 
  beacon.timestamp as epoch_ms
FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons`
WHERE 
  DATE(_PARTITIONTIME) is not null  
  and beacon.event_name in ('view_listing')
)
, userBrowserBucketing as(
SELECT 
  bucketing_id, 
  UNIX_MILLIS(bucketing_ts) as bucketing_ms,
  bucketing_id_type,
  STRUCT(bucketing_id AS bucketing_id, 
        bucketing_ts AS bucketing_ts, 
        experiment_id as experiment_id,
        UNIX_MILLIS(bucketing_ts) as bucketing_ms,
        bucketing_id_type as bucketing_id_type, 
        visit_id as visit_id, 
        sequence_number as sequence_number) AS bucket 
from (
  select *except(associated_ids)
  from 
    indexesOfInterest
  where 
    bucketing_id = user_id or bucketing_id = browser_id
    )
),
userBeaconsBuckets as (
SELECT 
  bucketing_id, 
  bucket, 
  beacon,
  epoch_ms - bucketing_ms as time_diff,
  listing_id from(
select * from
  (select cast(user_id as string) as bucketing_id,
  epoch_ms,
  listing_id,
  STRUCT(listing_id AS listing_id, 
         epoch_ms AS epoch_ms, 
         user_id as user_id, 
         browser_id as browser_id) AS beacon
from 
  ListingID
where 
  (user_id is not null) and cast(user_id as string) not in ("0", "")))
join
  (select * from userBrowserBucketing
  where bucketing_id_type = 2) 
   using(bucketing_id)
),
browserBeaconsBuckets as (
SELECT
  bucketing_id, 
  bucket, 
  beacon, 
  epoch_ms - bucketing_ms as time_diff,
  listing_id   
from (
    select 
      browser_id as bucketing_id,
      epoch_ms,
      listing_id,
    STRUCT(listing_id AS listing_id, 
           epoch_ms AS epoch_ms, 
           user_id as user_id, 
           browser_id as browser_id) AS beacon
from ListingID)
join
  (select * from userBrowserBucketing
  where bucketing_id_type = 1)
    using(bucketing_id)
),
bucketingListingID as(
SELECT
  bucketing_id,
  bucket.bucketing_ts as bucketing_ts,
  bucket.visit_id as visit_id, 
  bucket.bucketing_id_type as bucketing_id_type,
  bucket.sequence_number as sequence_number,
  ARRAY_AGG(listing_id ORDER BY ABS(time_diff) LIMIT 1)[OFFSET(0)] AS listing_id
From
  (select * from browserBeaconsBuckets
  union all 
  select * from userBeaconsBuckets)
WHERE
  time_diff BETWEEN  -86400000 AND 200  
GROUP BY
  bucketing_id, bucket
)
select
  -- _date as _date, 
  bucketing_id, 
  bucketing_ts,
  visit_id, 
  bucketing_id_type,
  sequence_number, 
  listing_id
from bucketingListingID
GROUP BY ALL
);
end
