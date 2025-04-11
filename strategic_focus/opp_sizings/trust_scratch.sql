/* OVERALL COVERAGES */
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

-- All web shop home traffic over last 30 days
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
  and e.event_type in ('shop_home')	

/* SHOP TRUST
-- surfaces: shop home page, 'meet the seller' on listing page */
select
  count(distinct v.visit_id) as visits_w_engagement,
  count(distinct case when v.converted > 0 then v.visit_id end) as visits_w_engagement_convert
from
  etsy-data-warehouse-prod.weblog.events e 
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where
  v._date >= current_date-30
	and event_type in ('shop_home','shop_owners_seen')
group by all 


/* REVIEWS
-- surfaces: reviews on listing page */
select
  count(distinct v.visit_id) as visits_w_engagement,
  count(distinct case when v.converted > 0 then v.visit_id end) as visits_w_engagement_convert
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` b 
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where
	date(_partitiontime) >= current_date-30
  and v.platform in ('mobile_web','destkop')
  and v._date >= current_date-30
	and ((beacon.event_name in ("listing_page_reviews_pagination","appreciation_photo_overlay_opened",'listing_page_reviews_content_toggle_opened') --all these events are lp specific 
      or (beacon.event_name) in ("sort_reviews") and (select value from unnest(beacon.properties.key_value) where key = "primary_event_source") in ('view_listing')))  -- sorting on listing page 
group by all 



/* LISTING SCANNABILITY
-- surfaces: listing page */


