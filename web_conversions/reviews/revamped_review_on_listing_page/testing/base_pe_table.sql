select bucketing_id, count(*) from etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_post_cattags
group by all order by 2 desc limit 5
/* 
bucketing_id	f0_
bKWZiJQwn1B48b5tqaHQPb0ZFgqi	340
BntJ8ePasWuSSRyrOTupfYcClH1m	306
0429950f4dda4b49ab8d61b80fba	291
cef979f025ba4c8fb00642c961d9	287
755cec8067934b3b88096f2d6dc0	274
*/

-- check to make sure the dates are after the bucketing date of 6/10
select visit_date, count(distinct visit_id) from etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_post_cattags where bucketing_id in ('bKWZiJQwn1B48b5tqaHQPb0ZFgqi') group by all order by 1 asc 
/* 
visit_date	f0_
2025-06-20	12
2025-06-21	12
2025-06-22	1
2025-06-23	16
2025-06-24	45
2025-06-25	62
2025-06-26	61
2025-06-27	120
2025-06-28	10
2025-06-29	1
*/

select distinct bucketing_date from etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_post_cattags where bucketing_id in ('bKWZiJQwn1B48b5tqaHQPb0ZFgqi') order by 1 asc  
-- 2025-06-20


-- select bucketing_id, visit_id, count(*) from etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_post_cattags group by all order by 3 desc limit 5
-- select bucketing_id, count(distinct bucketing_date) from etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_post_cattags group by all order by 2 desc limit 5
