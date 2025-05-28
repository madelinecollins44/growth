/* link to segmentation checks in BQ: https://docs.google.com/spreadsheets/d/19KuDMYtx9ydrVQ0d6Bmv5Zo1z14_FEvcxxwEoCs7vLc/edit?gid=2031585801#gid=2031585801 */

----------------------------------------------------------------------------------------------------------------------------------------------------------
/* LISTING_
Segmentation definition: Whether or not the first listing viewed during the experiment (i.e., the bucketing listing) is a physical or digital item 

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
  current_date-5 AS _date,
  v.visit_id,
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
  v._date = current_date-5
  AND lv._date = current_date-5
)
select
  segment_value,
  count(sequence_number) as listing_views,
  count(distinct visit_id) as visits
from 
  agg 
group by all 
/* segment_value	listing_views	visits
0	57865516	15915635
undefined	271	222
1	10948471	4469891 */
