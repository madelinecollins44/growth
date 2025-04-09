/* things to consider 
-- category/ subcategory purchases
----- digital/ pod? 
----- category count
----- digital/ pod? 

-- visit count (w/ listing views but no conversion)
----- converted visits
----- visits w/ lv

-- transactions
----- purchase days 
----- avg aov
----- time between purchases 

*/

with top10pct as (
select
    mapped_user_id,
    sum(gms_net) as total_gms,
    count(distinct transaction_id) as transactions,
    count(distinct trans_date) as purchase_dates
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms
inner join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile 
    using (user_id)
qualify percent_rank() over (order by total_gms desc) <= 0.10
-- qualify ntile(10) over (order by total_gms desc) = 1
order by 2 desc 
)
