/* All web traffic over last 30 days
-- platforms: desktop, mobile web */
select 
  count(distinct visit_id) as total_visits,
  count(distinct case when converted > 0 then visit_id end) as converted_visits,
  count(distinct case when converted > 0 then visit_id end) / count(distinct visit_id) as conversion_rate
from etsy-data-warehouse-prod.weblog.visits
where 
    platform in ('desktop','mobile_web')
  and _date >= current_date-30


/* Shop Trust
-- surfaces: shop home page, 'meet the seller' on listing page */

/* Reviews
-- surfaces: reviews on listing page */

/* Listing Scannability 
-- surfaces: listing page */

