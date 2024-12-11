---------------------------------------------------------------------------------------------------------------------------------------------
--What are the most used parts of the page? 
----Scroll depth, clicks, etc
----Segment by visitors that purchase in-session vs. not, purchase something from the shop vs. not
---------------------------------------------------------------------------------------------------------------------------------------------

select
  event_type,
  count(distinct visit_id) as visits
from etsy-data-warehouse-prod.weblog.events e  
inner join etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date-30
  and event_type like ('shop_home%')
  and v.platform in ('mobile_web','desktop')
-- and v.platform in ('boe')
group by all
