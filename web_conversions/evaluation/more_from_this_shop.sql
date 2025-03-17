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
