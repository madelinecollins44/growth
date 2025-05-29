--------------------------------------------------------------------------------------------------------
-- TESTING
--------------------------------------------------------------------------------------------------------
select
  listing_id,
  count(distinct transaction_id) as total_reviews,
  count(distinct case when date(transaction_date) >= current_date-365 then transaction_id end) reviews_in_last_year,
  count(distinct case when date(transaction_date) <= current_date-365 then transaction_id end) reviews_in_before_last_year
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews  
where 
  has_review > 0 -- only listings 
group by all 
having count(distinct transaction_id) >= 20
order by 2 desc limit 5

/* 
listing_id	reviews_in_last_year	reviews_in_before_last_year
273159520	0	1
229808336	0	1
1451134532	0	2
1541322607	0	1
576911234	0	1
1518307138	8927	2042
1167562350	7205	18701
1413455244	5941	11622
1678173188	5796	1805
1651955178	5188	527
*/
