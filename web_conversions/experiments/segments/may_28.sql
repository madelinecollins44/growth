/* link to segmentation checks in BQ: https://docs.google.com/spreadsheets/d/19KuDMYtx9ydrVQ0d6Bmv5Zo1z14_FEvcxxwEoCs7vLc/edit?gid=2031585801#gid=2031585801 */

----------------------------------------------------------------------------------------------------------------------------------------------------------
/* LISTING_
Segmentation definition: Whether or not the first listing viewed during the experiment (i.e., the bucketing listing) is a physical or digital item 
sample table in BQ: etsy-data-warehouse-dev.catapult_temp.segmentation_sample_run_is_digital_listing_1748447113
*/
----------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT 
  {{ input_run_date }} AS _date,
  v.visit_id,
  lv.sequence_number,
  COALESCE(l.is_digital, "undefined") AS segment_value
FROM
  `etsy-data-warehouse-prod.weblog.visits` AS v
LEFT JOIN
  `etsy-data-warehouse-prod.analytics.listing_views` AS lv USING (visit_id, _date)
LEFT JOIN
  `etsy-data-warehouse-prod.listing_mart.listing_attributes` AS l USING (listing_id)
WHERE
  v._date = {{ input_run_date }} 
  AND lv._date = {{ input_run_date }}
	
------TESTING
with agg as (
SELECT 
  current_date-1 AS _date,
  v.visit_id,
  v.browser_id,
  lv.sequence_number,
  l.listing_id,
  COALESCE(cast(l.is_digital as string), "undefined") AS segment_value
FROM
  `etsy-data-warehouse-prod.weblog.visits` AS v
LEFT JOIN
  `etsy-data-warehouse-prod.analytics.listing_views` AS lv USING (visit_id, _date)
LEFT JOIN
  `etsy-data-warehouse-prod.listing_mart.listing_attributes` AS l USING (listing_id)
WHERE
  v._date = current_date-1
  AND lv._date = current_date-1
)
select
  segment_value,
  count(sequence_number) as listing_views,
  count(distinct visit_id) as visits,
  count(distinct browser_id) as browser_id
from 
  agg 
group by all 
/*
segment_value	listing_views	visits	browser_id
1	10948471	4469891	3741097
undefined	271	222	216
0	57865516	15915635	12649048 */

	
select
  segment_value,
  count(sequence_number) as listing_views,
  count(distinct visit_id) as visits,
  count(distinct bucketing_id) as bucketing_id
from 
  etsy-data-warehouse-dev.catapult_temp.segmentation_sample_run_is_digital_listing_1748447113 
group by all 
order by 1 desc 
/* segment_value	listing_views	visits	bucketing_id
undefined	91953312	25626016	28605735
1	3243382	3071000	3194487
0	10429304	9728092	10056744
*/

-- check again listing page experiment 
WITH experiment_bucketing_units AS (
  SELECT *
  FROM `etsy-data-warehouse-dev.catapult_temp.segmentation_sample_run_is_digital_listing_1748447113`
    INNER JOIN `etsy-data-warehouse-prod.catapult_unified.bucketing_period` USING(_date, bucketing_id, bucketing_id_type, bucketing_ts)
  WHERE experiment_id = 'growth_regx.lp_move_appreciation_photos_mweb'
)
SELECT  
  segment_value, 
  COUNT(*) AS total_bucketing_units,
  COUNT(*) / (SELECT COUNT(*) FROM experiment_bucketing_units)
FROM experiment_bucketing_units
GROUP BY 1
ORDER BY 1 DESC
	
----------------------------------------------------------------------------------------------------------------------------------------------------------
/* RECENT SHOP HOME VISITS 
Segmentation definition:

*/
----------------------------------------------------------------------------------------------------------------------------------------------------------
-- segmentations 
with unit_shop_home_views as (
    -- browser bucketed tests
    select 
      {{input_run_date}} as _date,
      split(lv.visit_id, ".")[0] as bucketing_id, 
      1 as bucketing_id_type, 
      count(distinct concat(lv.visit_id, lv.sequence_number)) as shop_home_views
    from 
      etsy-data-warehouse-prod.weblog.events e
    where 
      lv._date between DATE_SUB({{input_run_date}}, INTERVAL 14 DAY) and {{input_run_date}} 
        and event_type in ('shop_home')
    group by all
    union all 
    -- user bucketed_tests 
    select 
      {{input_run_date}} as _date,
      cast(v.user_id as string) as bucketing_id, 
      2 as bucketing_id_type, 
      count(distinct concat(lv.visit_id, lv.sequence_number)) as shop_home_views
    from etsy-data-warehouse-prod.weblog.events e
    left join `etsy-data-warehouse-prod.weblog.visits` v 
      on v.visit_id = lv.visit_id
    where 
      lv._date between DATE_SUB({{input_run_date}}, INTERVAL 14 DAY) and {{input_run_date}} 
      and event_type in ('shop_home')
      and v._date = {{input_run_date}}
    group by all
)
select 
  _date,            
  bucketing_id, 
  bucketing_id_type,
  case 
      when shop_home_views between 1 and 10 then '1-10'
      when shop_home_views between 11 and 20 then '11-20'
      when shop_home_views between 21 and 30 then '21-30'
      when shop_home_views between 31 and 40 then '31-40'
      when shop_home_views between 41 and 50 then '41-50'
     else '50_or_more' end as segment_value
  from unit_shop_home_views
	
------TESTING
