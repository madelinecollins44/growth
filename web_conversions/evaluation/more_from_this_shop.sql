--------------------------------------------------------------------------------
-- WHAT % OF LISTING VIEWS SEE THE 'MORE FROM THIS SHOP' MODULE? 
--------------------------------------------------------------------------------
-- MFTS seen events 
select
	visit_id,
	sequence_number,
	beacon.event_name as event_name,
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
	date(_partitiontime) >= current_date-30
and
	beacon.event_name in ("recommendations_module_seen")
	and (select value from unnest(beacon.properties.key_value) where key = "module_placement") in ("listing_side")

-- listing views from the MFTS module (clicks on module)
select
  count(distinct visit_id) as visits,
  count(sequence_number) as listing_views
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  platform in ('mobile_web','desktop')
  and ref_tag like ('related%')

--------------------------------------------------------------------------------
-- TESTING
--------------------------------------------------------------------------------
-- test 1: see if target_listing_id or page_listing_id is better to extract listing_id from MFTS module
select
  count(sequence_number) as mfts_modules_seen,
	count((select value from unnest(beacon.properties.key_value) where key = "target_listing_id")) as target_listing_id, 
  count((select value from unnest(beacon.properties.key_value) where key = "page_listing_id")) as page_listing_id, 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
inner join 
  etsy-data-warehouse-prod.weblog.visits using (visit_id)
where
	date(_partitiontime) >= current_date-30
  and _date >= current_date-30
	and (beacon.event_name in ("recommendations_module_seen") and (select value from unnest(beacon.properties.key_value) where key = "module_placement") in ("listing_side")) -- MFTS modules 
  and platform in ('mobile_web','desktop')
group by all 
