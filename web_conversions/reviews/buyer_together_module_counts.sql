---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--There is a 'buy together, get...' module that sometimes surfaces, I think it can be a sale module and sometimes it surfaces as free shipping, but we were wondering what % of listings have it. 
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- free shipping: https://www.etsy.com/listing/1728673967/macrame-table-placemats-natural-white
-------- event name: recommendations_module_seen, property: lp_free_shipping_bundle

-- get x% off: https://www.google.com/url?q=https://www.etsy.com/listing/1581168558/upcycled-fabric-handmade-boho-round&sa=D&source=editors&ust=1738007917145372&usg=AOvVaw3umrGY6l-Did44U-kSBTL2
-------- event name: mix_and_match_v2_bundle_lp_shown

with listing_events as (
select
	date(_partitiontime) as _date,
	visit_id,
  platform,
  sequence_number,
	beacon.event_name as event_name,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
inner join 
  etsy-data-warehouse-prod.weblog.visits using (visit_id)
where
	date(_partitiontime) >= current_date-30
  and _date >= current_date-30
	and 
    (beacon.event_name in ("view_listing","mix_and_match_v2_bundle_lp_shown") -- listing view, discount bundle
    or (beacon.event_name in ("recommendations_module_seen") and (select value from unnest(beacon.properties.key_value) where key = "module_placement") in ("lp_free_shipping_bundle"))) -- free shipping
  and platform in ('mobile_web','desktop')
group by all 
)
, ordered_events as (
select
	_date,
	visit_id,
  platform,
	listing_id,
  sequence_number,
	event_name,
	lead(event_name) over (partition by visit_id, listing_id order by sequence_number) as next_event,
	lead(sequence_number) over (partition by visit_id, listing_id order by sequence_number) as next_sequence_number
from 
	listing_events
)
, listing_views as (
select
	visit_id,
  platform,
	listing_id,
  sequence_number,
	case when next_event in ('recommendations_module_seen') then 1 else 0 end as saw_free_shipping_bundle, 
  case when next_event in ('mix_and_match_v2_bundle_lp_shown') then 1 else 0 end as saw_discount_bundle
from
	ordered_events
where 
	event_name in ('view_listing')
)
select
  count(visit_id) as listing_views,
  count(case when saw_free_shipping_bundle > 0 then visit_id end) as lv_w_free_shipping_bundle,
  count(case when saw_discount_bundle > 0 then visit_id end) as lv_w_discount_bundle,
  count(distinct listing_id) as listings_viewed,
  count(distinct case when saw_free_shipping_bundle > 0 then listing_id end) as listings_w_free_shipping_bundle,
  count(distinct case when saw_discount_bundle > 0 then listing_id end) as listings_w_discount_bundle,
from 
  listing_views

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- testing 
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-
