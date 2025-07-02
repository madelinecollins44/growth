select count(distinct bucketing_id) from etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_listing_views 
-- 25261261
select count(distinct bucketing_id) from etsy-data-warehouse-dev.madelinecollins.ab_first_bucket 
--26530476

select bucketing_id, count(*) from etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_listing_views  group by all order by 2 desc limit 6
/* 
bucketing_id	f0_
a-4Wgi38VMkhLmL-_aJfU0Q3vdhB	23474
ZY_PbdFqIbQp6ZckOzQXupuOML0q	18450
3ctzT-ofyXOUb8kc50pS-kFG9rrl	11252
QOBgSJXup5_Nm6aIqREP-OOkmnzy	10512
49hsnHN2t0zd86YanathgzalGJCq	8549
7CUhnfyWJMBcFsojRh1s7eXj3ElW	5117
*/

select 
  case when split(a.visit_id, ".")[0] is null then 1 else 0 end as no_lv,
  count(distinct bucketing_id) 
from 
  etsy-data-warehouse-dev.madelinecollins.ab_first_bucket v
left join 
  etsy-data-warehouse-prod.analytics.listing_views  a
    on split(a.visit_id, ".")[0] = v.bucketing_id 
    and a._date >= date('2025-06-10')
-- where a._date is not null 
group by all
/* 
no_lv	f0_
0	25261398
1	1269078

select (25261398+1269078)/ 26530476
*/

