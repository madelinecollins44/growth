
select 
 CASE 
    when time_until_review < 0 then 'Before first eligible day'
    when time_until_review = 0 then 'First eligible day'
    when time_until_review = 1 then 'Day after first eligible day'
    when time_until_review between 2 and 6 then 'Week 1: 2-6 days'
    when time_until_review between 7 and 13 then 'Week 2: 7-13 days'
    when time_until_review between 14 and 20 then 'Week 3: 14-20 days'
    when time_until_review between 21 and 27 then 'Week 4: 21-27 days'
    when time_until_review between 28 and 59 then 'Month 2: 28-59 days'
    when time_until_review between 60 and 89 then 'Month 3: 60-89 days'
    when time_until_review between 90 and 119 then 'Month 4: 90-119 days'
    when time_until_review between 120 and 149 then 'Month 5: 120-149 days'
    when time_until_review >= 155940131130 then 'Over 150 days'
    when review_start is null then 'Review start is null'
    ELSE 'Error'
  end as time_eligible,
  count(distinct transaction_id) as total_transactions 
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
where 
  has_review =1
group by all 
order by 1 asc



  
  --testing to see how many transactions are missing review_start but have time_until_review
select 
  count(distinct transaction_id) as total_reviews,
  count(distinct case when review_start is null then transaction_id end) as trans_wo_review_start,
  count(distinct case when time_until_review is null then transaction_id end) as trans_wo_time_until_review,
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
where 
  has_review =1
--594013113 transactions
--213200992 transactions wo review start
--213200992 transactions wo time until review
-- 36% of transactions with reviews dont have a review start date



--testing to make sure day calc is right
--100 day calc
select transaction_id, date(review_date), date(review_start) from  etsy-data-warehouse-prod.rollups.transaction_reviews
where transaction_id in (1451147833,2030692255,2039775796,2078557855)

-- 1 day calc
select transaction_id, date(review_date), date(review_start) from  etsy-data-warehouse-prod.rollups.transaction_reviews
where transaction_id in (1224390424,1246873023,1251576675,1293327512)
