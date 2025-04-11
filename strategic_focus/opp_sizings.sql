/* OVERALL TRAFFIC COUNTS */
-- All web traffic over last 30 days
select 
  count(distinct visit_id) as total_visits,
  count(distinct case when converted > 0 then visit_id end) as converted_visits,
  count(distinct case when converted > 0 then visit_id end) / count(distinct visit_id) as conversion_rate
from etsy-data-warehouse-prod.weblog.visits
where 
    platform in ('desktop','mobile_web')
  and _date >= current_date-30

-- All web shop home / view listing traffic over last 30 days
select 
  count(distinct visit_id) as total_visits,
  count(distinct case when converted > 0 then visit_id end) as converted_visits,
  count(distinct case when converted > 0 then visit_id end) / count(distinct visit_id) as conversion_rate
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 1=1
  and platform in ('mobile_web','desktop')
  and v._date >= current_date-30
  and e.event_type in ('shop_home','view_listing')

-- All web view listing traffic over last 30 days
select 
  count(distinct visit_id) as total_visits,
  count(distinct case when converted > 0 then visit_id end) as converted_visits,
  count(distinct case when converted > 0 then visit_id end) / count(distinct visit_id) as conversion_rate
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 1=1
  and platform in ('mobile_web','desktop')
  and v._date >= current_date-30
  and e.event_type in ('view_listing')

/* SHOP TRUST
-- surfaces: shop home page, 'meet the seller' on listing page */

/* REVIEWS
-- surfaces: reviews on listing page */

/* LISTING SCANNABILITY
-- surfaces: listing page */

