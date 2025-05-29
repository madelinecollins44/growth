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

etsy-data-warehouse-dev.catapult_temp.segmentation_sample_run_shop_home_views_last_14d_1748526318
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
      when shop_home_views = 0 then '0'
      when shop_home_views = 1 then '1'
      when shop_home_views = 2 then '2'
      when shop_home_views = 3 then '3'
      when shop_home_views = 4 then '4'
      when shop_home_views = 5 then '5'
      when shop_home_views between 6 and 10 then '6-10'
      when shop_home_views between 11 and 20 then '11-20'
     else '20_or_more' 
end as segment_value,
from unit_shop_home_views
	
------TESTING
select
  case when event_type in ('shop_home') then 1 else 0 end as shop_home_traffic,
  count(distinct visit_id) as visits, 
  -- count(distinct (split(visit_id, "."))) as browsers
from 
  etsy-data-warehouse-prod.weblog.events
where _date >= current_date-14
group by all 
/*	shop_home_traffic	visits
1	60627567
0	493282546 */

-- TESTING ON BROWSER LEVEL 
with unit_shop_home_views as (
    -- browser bucketed tests
    select 
      current_date as _date,
      split(e.visit_id, ".")[0] as bucketing_id, 
      1 as bucketing_id_type, 
      count(distinct concat(e.visit_id, e.sequence_number)) as shop_home_views
    from 
      etsy-data-warehouse-prod.weblog.events e
    where 
      e._date between DATE_SUB(current_date, INTERVAL 14 DAY) and current_date 
        and event_type in ('shop_home')
    group by all
    union all 
    -- user bucketed_tests 
    select 
      current_date as _date,
      cast(v.user_id as string) as bucketing_id, 
      2 as bucketing_id_type, 
      count(distinct concat(e.visit_id, e.sequence_number)) as shop_home_views
    from etsy-data-warehouse-prod.weblog.events e
    left join `etsy-data-warehouse-prod.weblog.visits` v 
      on v.visit_id = e.visit_id
    where 
      e._date between DATE_SUB(current_date, INTERVAL 14 DAY) and current_date 
      and event_type in ('shop_home')
      and v._date = current_date
    group by all
)
select 
  _date,            
  bucketing_id, 
  bucketing_id_type,
  case 
      when shop_home_views = 0 then '0'
      when shop_home_views = 1 then '1'
      when shop_home_views = 2 then '2'
      when shop_home_views = 3 then '3'
      when shop_home_views = 4 then '4'
      when shop_home_views = 5 then '5'
      when shop_home_views between 6 and 10 then '6-10'
      when shop_home_views between 11 and 20 then '11-20'
     else '20_or_more' 
end as segment_value,
from unit_shop_home_views
QUALIFY ROW_NUMBER() OVER (PARTITION BY segment_value ORDER BY RAND()) = 5
/* _date	bucketing_id	bucketing_id_type	segment_value
2025-05-29	zhysWAX2pX7wDMeidcFZ6FPUAL1n	1	5
2025-05-29	4B664D0A53934B3D8D05F0BD4C15	1	20_or_more
2025-05-29	85916368B4AF489C8F2C8200440C	1	11-20
2025-05-29	5AD69991D1334B32ABEFAD93F36C	1	4
2025-05-29	lLF3XyIBOa11yubDBv6o5jBiWVyO	1	6-10
2025-05-29	gBrruBNI22IWf7aNaLZ0AOVvDH3J	1	3
2025-05-29	hd7zXHDPL7VbK5JXX1ejvBBNv6OL	1	2
2025-05-29	jZUJ_6JvZwinhaWWOSsrKJPtuXUT	1	1
2025-05-29	319635185	2	5
2025-05-29	11004342	2	4
2025-05-29	782914317	2	11-20
2025-05-29	10487214	2	20_or_more
2025-05-29	289727567	2	3
2025-05-29	52009954	2	6-10
2025-05-29	1024977062	2	1
2025-05-29	1093259138	2	2 */ 

select 
	event_type, 
	split(visit_id, ".")[0] as browser_id, 
	count(sequence_number) 
from 
	etsy-data-warehouse-prod.weblog.events 
where split(visit_id, ".")[0] in 
	('zhysWAX2pX7wDMeidcFZ6FPUAL1n','4B664D0A53934B3D8D05F0BD4C15','85916368B4AF489C8F2C8200440C','5AD69991D1334B32ABEFAD93F36C','lLF3XyIBOa11yubDBv6o5jBiWVyO','gBrruBNI22IWf7aNaLZ0AOVvDH3J','hd7zXHDPL7VbK5JXX1ejvBBNv6OL','jZUJ_6JvZwinhaWWOSsrKJPtuXUT') 
and _date between DATE_SUB(current_date, INTERVAL 14 DAY) and current_date 
and event_type in ('shop_home') group by all 
/* event_type	browser_id	f0_
shop_home	zhysWAX2pX7wDMeidcFZ6FPUAL1n	5
shop_home	hd7zXHDPL7VbK5JXX1ejvBBNv6OL	2
shop_home	5AD69991D1334B32ABEFAD93F36C	4
shop_home	jZUJ_6JvZwinhaWWOSsrKJPtuXUT	1
shop_home	lLF3XyIBOa11yubDBv6o5jBiWVyO	10
shop_home	gBrruBNI22IWf7aNaLZ0AOVvDH3J	3
shop_home	85916368B4AF489C8F2C8200440C	12
shop_home	4B664D0A53934B3D8D05F0BD4C15	50
shop_home	10487214	26
shop_home	289727567	3
shop_home	782914317	12
shop_home	319635185	5
shop_home	52009954	6
shop_home	1093259138	2
shop_home	11004342	4
shop_home	1024977062	1	
	*/


	
-- testing overall traffic counts	
select
  segment_value,
  bucketing_id_type,
  count(distinct bucketing_id) as visits,
from 
  etsy-data-warehouse-dev.catapult_temp.segmentation_sample_run_shop_home_views_last_14d_1748526318
group by all 
order by 1 desc 

-- testing against specific experiments 
WITH experiment_bucketing_units AS (
  SELECT *
  FROM etsy-data-warehouse-dev.catapult_temp.segmentation_sample_run_shop_home_views_last_14d_1748526318
    INNER JOIN `etsy-data-warehouse-prod.catapult_unified.bucketing_period` USING(_date, bucketing_id, bucketing_id_type, bucketing_ts)
  WHERE experiment_id = 'growth_regx.lp_move_appreciation_photos_mweb'
)
SELECT  
  segment_value, 
  COUNT(*) AS total_bucketing_units,
  COUNT(*) / (SELECT COUNT(*) FROM experiment_bucketing_units)
FROM experiment_bucketing_units
GROUP BY 1
ORDER BY 1 ASC
	

-- testing against specific experiment 
WITH experiment_bucketing_units AS (
  SELECT *
  FROM etsy-data-warehouse-dev.catapult_temp.segmentation_sample_run_shop_home_views_last_14d_1748466803
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
	

