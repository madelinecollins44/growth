-- --listing views of high stake vs low stake 
-- --listing views of high stake vs low stake 
with views as (
select
  _date, 
  listing_id,
  visit_id, 
  case
    when price_usd > 100 then 'high stakes'
    else 'low stakes'
    end as listing_type,
  count(visit_id) as listing_views,
  sum(purchased_after_view) as purchased_after_view
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
group by all 
)
, seen_reviews as (
select
	date(_partitiontime) as _date,
	visit_id,
  regexp_extract(beacon.loc, r'listing/(\d+)') as listing_id,
  count(visit_id) as reviews_event_seen,
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
	date(_partitiontime) >= current_date-4
	and beacon.event_name = "listing_page_reviews_seen"
group by all 
)
, number_of_reviews as (
select
  listing_id,
  listing_rating_count,
  shop_rating_count,
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
qualify row_number() over (partition by listing_id order by _date desc) = 1
)



---------------------------------------------------------------
--TESTING
---------------------------------------------------------------
, reviews as (
select
	date(_partitiontime) as _date,
	visit_id,
  regexp_extract(beacon.loc, r'listing/(\d+)') as listing_id,
  count(visit_id) as reviews_event_seen,
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
	date(_partitiontime) >= current_date-4
	and beacon.event_name = "listing_page_reviews_seen"
  and visit_id in ('rusJzZ2CpDruwYkS4Jh4XdR9W9eF.1734370468821.5')
group by all 
-- _date	visit_id	f0_	reviews_event_seen	reviews_event_seen_2
-- 2024-12-16	rusJzZ2CpDruwYkS4Jh4XdR9W9eF.1734370468821.5	1743145166	884	884
-- 2024-12-16	rusJzZ2CpDruwYkS4Jh4XdR9W9eF.1734370468821.5	999286621	2	2

  -- TEST AGAIN WEBLOG.EVENTS 
select count(visit_id) from etsy-data-warehouse-prod.weblog.events
where event_type = "listing_page_reviews_seen"
and visit_id in ('rusJzZ2CpDruwYkS4Jh4XdR9W9eF.1734370468821.5')
--886
