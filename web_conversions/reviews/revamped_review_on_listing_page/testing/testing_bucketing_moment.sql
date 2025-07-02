-- make sure the new way of finding bucketing moment matches old way 
with bucketing as ( -- pulls out bucketing moment 
SELECT 
  _date,
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
select *from bucketing where bucketing_id in ('zzzzw7Yi_qI3RzxjMXtTamc7d5Zc','zzzyw63RhThcGxObiOXHlRa7FY7m','zzzycMhoUVhIyR9HVtr2H67KPrma')
/* 
bucketing_id	f0_
zzzzw7Yi_qI3RzxjMXtTamc7d5Zc	1
zzzyw63RhThcGxObiOXHlRa7FY7m	1
zzzycMhoUVhIyR9HVtr2H67KPrma	1
zzzxkufU7_A8VCbSSdppu9O23mlS	1
zzzxT6nnFq7XNI19r4Qnohvo9vK0	1
zzzx69eNOhNs31shWIM1ldW-8snl	1
zzzwxjTNxus-FFm-VoyPVec2cSus	1
zzzwkidROJ9UBtvgdy1Xlc_RF0Tm	1
zzzwheTfAM9w5Gy9d7et60hr6TwG	1
zzzwffPgzCTmyF18Tvl-TM0HEuJF	1
*/





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

with agg as (
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
)
select * from agg where bucketing_id in ('zzzzw7Yi_qI3RzxjMXtTamc7d5Zc','zzzyw63RhThcGxObiOXHlRa7FY7m','zzzycMhoUVhIyR9HVtr2H67KPrma')


/* 
bucketing_id	bucketing_id_type	variant_id	bucketing_date	bucketing_ts
zzzycMhoUVhIyR9HVtr2H67KPrma	1	off	2025-06-10	2025-06-10 07:01:06.872000 UTC
zzzzw7Yi_qI3RzxjMXtTamc7d5Zc	1	on	2025-06-28	2025-06-28 19:07:38.970000 UTC
zzzyw63RhThcGxObiOXHlRa7FY7m	1	on	2025-05-12	2025-05-12 16:02:07.570000 UTC
*/
