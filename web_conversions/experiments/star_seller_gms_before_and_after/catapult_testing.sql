---------------------------------------------------------------------------------------------------------
-- TEST 1: making sure only views post bucketing are included
---------------------------------------------------------------------------------------------------------
with agg as (
select
  browser_id,
  bucket_date,
  visit_date,
  DATE_DIFF(CAST(bucket_date AS DATE), CAST(visit_date AS DATE), DAY) as difference
from etsy-data-warehouse-dev.madelinecollins.holder_table
group by 1,2,3
)
select 
  *, 
  count(distinct bucket_date) as bucket_dates, 
  count(distinct visit_date) as visit_dates
from agg 
where difference > 0 
group by 1,2,3,4
order by difference desc
limit 5
/* browser_id	bucket_date	visit_date	difference	bucket_dates	visit_dates
e1i-6a_hnYYH1AQr3KSHG7pfsl8K	2025-06-24	2025-06-16	8	1	1
vG107fu_eEkpYcSmvU2deF10yfVo	2025-06-24	2025-06-16	8	1	1
6SxAts_-P1-d7_iY755M-V_7r_3Z	2025-06-24	2025-06-16	8	1	1
6fzSbEET9CkWvpOL-w5-6cf8YK_N	2025-06-24	2025-06-16	8	1	1
Cg4TM1vH3ctPooyeulbXhXPHU6qj	2025-06-24	2025-06-16	8	1	1 */

select * from etsy-data-warehouse-dev.madelinecollins.holder_table where browser_id in ('e1i-6a_hnYYH1AQr3KSHG7pfsl8K')
/* first_bucket_date	visit_date	browser_id	variant_id	shop_id	visits
2025-06-17	2025-06-17	e1i-6a_hnYYH1AQr3KSHG7pfsl8K	off	12019805	3
2025-06-17	2025-06-17	e1i-6a_hnYYH1AQr3KSHG7pfsl8K	off	12302197	3
2025-06-17	2025-06-17	e1i-6a_hnYYH1AQr3KSHG7pfsl8K	off	14871416	3
2025-06-17	2025-06-17	e1i-6a_hnYYH1AQr3KSHG7pfsl8K	off	39750899	12
2025-06-17	2025-06-18	e1i-6a_hnYYH1AQr3KSHG7pfsl8K	off	39750899	3
2025-06-17	2025-06-24	e1i-6a_hnYYH1AQr3KSHG7pfsl8K	off	5190089	3
2025-06-17	2025-06-24	e1i-6a_hnYYH1AQr3KSHG7pfsl8K	off	14871416	3
2025-06-17	2025-06-24	e1i-6a_hnYYH1AQr3KSHG7pfsl8K	off	50778474	3
------------- ABOVE, 6.16 IS INCLUDED HOWEVER, THAT IS EXLCLUDED IN THIS CTE SINCE 6.16 WAS BEFORE BUCKETING DATE.
*/
