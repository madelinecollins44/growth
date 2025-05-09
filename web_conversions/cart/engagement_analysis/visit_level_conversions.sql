-------------------------------------------------------------------------------------------
-- WHAT DOES CONVERSION LOOK LIKE ACROSS # OF ITEMS ADDED TO CART? 
-------------------------------------------------------------------------------------------
select
  cart_adds,
  count(distinct visit_id) as visits,
  sum(converted) as conversions
from 
  etsy-data-warehouse-dev.madelinecollins.cart_engagement_browsers
group by all 
order by 1 asc 
