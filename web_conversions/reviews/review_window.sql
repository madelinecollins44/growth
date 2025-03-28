--------------------------------------------------------
--total transactions w reviews
--------------------------------------------------------
select count(distinct transaction_id) from  etsy-data-warehouse-prod.rollups.transaction_reviews where has_review =1 and date(transaction_date) >= current_date-365
 
--------------------------------------------------------
--breaking down transactions by review windows 
--------------------------------------------------------
-- using time_until_review from table
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
  and date(transaction_date) >= current_date-365
group by all 
order by 1 asc

-- -- using combo of review start + shipping date to find review eligible date
--  with reviews as (
-- select
--   transaction_id,
--   coalesce(date(review_start), date(shipped_date)) as review_start_date, -- if a transaction does not have a review start, then use the shipping date 
--   datetime_diff(date(review_date), (coalesce(date(review_start), date(shipped_date))), day) AS time_until_review, -- find date diff calcs for these situations 
-- from 
--   etsy-data-warehouse-prod.rollups.transaction_reviews
-- where 
--   has_review =1
-- group by all 
-- )
-- select 
--  CASE 
--     when time_until_review < 0 then 'Before first eligible day'
--     when time_until_review = 0 then 'First eligible day'
--     when time_until_review = 1 then 'Day after first eligible day'
--     when time_until_review between 2 and 6 then 'Week 1: 2-6 days'
--     when time_until_review between 7 and 13 then 'Week 2: 7-13 days'
--     when time_until_review between 14 and 20 then 'Week 3: 14-20 days'
--     when time_until_review between 21 and 27 then 'Week 4: 21-27 days'
--     when time_until_review between 28 and 59 then 'Month 2: 28-59 days'
--     when time_until_review between 60 and 89 then 'Month 3: 60-89 days'
--     when time_until_review between 90 and 119 then 'Month 4: 90-119 days'
--     when time_until_review between 120 and 149 then 'Month 5: 120-149 days'
--     when time_until_review >= 155940131130 then 'Over 150 days'
--     when review_start_date is null then 'Review start is null'
--     ELSE 'Error'
--   end as time_eligible,
--   count(distinct transaction_id) as total_transactions 
-- from 
--   reviews
-- group by all 
-- order by 1 asc

--find average windows
 with transaction_breakout as (
select
  transaction_id,
  time_until_review as time_until_review_from_table,
  datetime_diff(date(review_date), (coalesce(date(review_start), date(shipped_date))), day) AS time_until_review_coalesce -- find date diff calcs for these situations 
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
where 
  has_review =1
  and date(transaction_date) >= current_date-365
group by all 
)
select
  avg(time_until_review_from_table) as avg_time_until_review_from_table,
  avg(time_until_review_coalesce) as avg_time_until_review_coalesce
from 
  transaction_breakout

--------------------------------------------------------
--comparing delivery dates to review start date 
--------------------------------------------------------
select
  date_diff(sb.delivered_date, cast(review_start as date), day) AS time_until_review,
  count(distinct tr.receipt_id) as receipts
  -- date(tr.review_start) as review_start_date,
  -- sb.delivered_date
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews tr
left join 
  etsy-data-warehouse-prod.rollups.receipt_shipping_basics sb using (receipt_id)
where 
  date(transaction_date) >= current_date-365
  -- and time_until_review = -1 -- looking at reviews that submitted before window
group by all 
order by 2 desc

--checking on receipt level
 select
  sb.delivered_date as delivery,
  cast(review_start as date) as review_start,
  date_diff(sb.delivered_date, cast(review_start as date), day) AS time_until_review,
  receipt_id,
  -- date(tr.review_start) as review_start_date,
  -- sb.delivered_date
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews tr
left join 
  etsy-data-warehouse-prod.rollups.receipt_shipping_basics sb using (receipt_id)
where 
  date(transaction_date) >= current_date-365
  -- and time_until_review = -1 -- looking at reviews that submitted before window
group by all 
having date_diff(sb.delivered_date, cast(review_start as date), day)  = -1
order by 2 desc

--------------------------------------------------------
--understanding null review start dates
--------------------------------------------------------
-- yearly breakdown 
select 
  extract(year from date(review_date)) as year,
  count(distinct transaction_id) 
from  
  etsy-data-warehouse-prod.rollups.transaction_reviews 
where  
  has_review =1
  and review_start is null
group by all 
order by 1 asc

-- is download
 select 
  is_download,
  count(distinct transaction_id) 
from  
  etsy-data-warehouse-prod.rollups.transaction_reviews 
left join 
  etsy-data-warehouse-prod.listing_mart.listings using (listing_id)
where  
  has_review =1
  and review_start is null
group by all 
order by 1 asc

--top category
 select 
  top_category,
  count(distinct transaction_id) 
from  
  etsy-data-warehouse-prod.rollups.transaction_reviews 
where  
  has_review =1
  and review_start is null
group by all 
order by 1 asc

--------------------------------------------------------------
-- testing review before shipping, see if they are downloads 
--------------------------------------------------------------
  select 
  is_download,
  count(distinct transaction_id) 
from  
  etsy-data-warehouse-prod.rollups.transaction_reviews 
left join 
  etsy-data-warehouse-prod.listing_mart.listings using (listing_id)
where  
  has_review =1
  and date(shipped_date) <= date(review_date)
  and time_until_review < 0
group by all 
order by 1 asc
 
--------------------------------------------------------
-- testing by time_eligible group
--------------------------------------------------------
-- Before first eligible day: 2532165502, 3270327411, 4261574215, 2441851082, 3191477284
 select * from etsy-data-warehouse-prod.rollups.transaction_reviews where transaction_id in (2532165502, 3270327411, 4261574215, 2441851082, 3191477284)


-- Over 150 days: 3514811192, 3772148010, 4165919120, 4141564568, 2172709698
 select * from etsy-data-warehouse-prod.rollups.transaction_reviews where transaction_id in (3514811192, 3772148010, 4165919120, 4141564568, 2172709698)
--makes up 0% of all transactions, dont really need to worry here. 

 
----------------------------------------------------------------------------------------------------------------
-- in the before first eligible day category, how many review dates happen after the shipped dates?
 ----these are likely due to the fact that the item was recieved before review window opened (review window is calculated)
----------------------------------------------------------------------------------------------------------------
select 
  count(distinct case when date(shipped_date) <= date(review_date) then transaction_id end) as review_after_shipping, --reviews submitted before review window, but still after shipping date
  count(distinct case when date(shipped_date) > date(review_date) then transaction_id end) as review_before_shipping, --reviews submitted before review window, and before shipping date
  count(distinct transaction_id) as total_reviews
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
where 
  has_review =1
  and time_until_review < 0 -- reviews submitted before the review start date was eligible 
group by all 
order by 1 asc
-- review_after_shipping	review_before_shipping	total_reviews
-- 34063210	11936	34195213

 --what does it look like in cases where the review was left before the item was shipped? 
--------------------------------------------------------
-- testing other errors
--------------------------------------------------------  
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
