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
	date(_partitiontime) >= current_date-14
  and _date >= current_date-14
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
-- testing: counts from weblog.events  
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
select 
  event_type,
  count(visit_id) as instances 
from etsy-data-warehouse-prod.weblog.events
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where v._date >= current_date-14
and platform in ('mobile_web','desktop')
and event_type in ("view_listing","mix_and_match_v2_bundle_lp_shown")
group by all
-- event_type	instances
-- mix_and_match_v2_bundle_lp_shown	18494184
-- view_listing	535617857

select
	count(visit_id)
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
inner join 
  etsy-data-warehouse-prod.weblog.visits using (visit_id)
where
	date(_partitiontime) >= current_date-14
  and _date >= current_date-14
	and (beacon.event_name in ("recommendations_module_seen") and (select value from unnest(beacon.properties.key_value) where key = "module_placement") in ("lp_free_shipping_bundle")) -- free shipping
  and platform in ('mobile_web','desktop')
group by all 
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- testing: next event 
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- free shipping:
-- _date	visit_id	platform	listing_id	sequence_number	event_name	next_event	next_sequence_number
-- 2025-01-26	-1iXIdf-5j5pGuwCiiAr45wfz09m.1737894729108.2	desktop	1178663538	20	view_listing	recommendations_module_seen	63
-- 2025-01-26	-268ArIp0nmQSrHqct8U-2ur02G1.1737856103911.2	desktop	1784603062	0	view_listing	recommendations_module_seen	53
-- 2025-01-25	-2foH3yBcetFNjzsgQkH7eeArLhD.1737829170870.1	desktop	1175626115	78	view_listing	recommendations_module_seen	130
select * from etsy-data-warehouse-prod.weblog.events where _date >= current_date-4 and visit_id in ('-1iXIdf-5j5pGuwCiiAr45wfz09m.1737894729108.2') and sequence_number >= 20 order by sequence_number asc

	
-- discount bundle:
-- _date	visit_id	platform	listing_id	sequence_number	event_name	next_event	next_sequence_number
-- 2025-01-26	--2K0pqZ60MffTrc-ETQXAwdzX1e.1737929685390.2	desktop	1454084488	494	view_listing	mix_and_match_v2_bundle_lp_shown	504
-- 2025-01-25	--6_htKqQZAd2O34xlR9b4r37XdQ.1737848190145.2	desktop	1261533844	0	view_listing	mix_and_match_v2_bundle_lp_shown	8
-- 2025-01-25	--MoGjrZsIrgDGZMYEbmiErkKA4B.1737821111981.1	desktop	1772885858	19	view_listing	mix_and_match_v2_bundle_lp_shown	25
select * from etsy-data-warehouse-prod.weblog.events where _date >= current_date-4 and visit_id in ('--2K0pqZ60MffTrc-ETQXAwdzX1e.1737929685390.2') and sequence_number >= 494 and event_type in ('view_listing','mix_and_match_v2_bundle_lp_shown') order by sequence_number asc


-- view listing:
-- _date	visit_id	platform	listing_id	sequence_number	event_name	next_event	next_sequence_number
-- 2025-01-26	---aCiaj3WISnbLus7rgP_KsIHsD.1737879603072.2	mobile_web	645298964	105	view_listing	view_listing	215
-- 2025-01-26	---aCiaj3WISnbLus7rgP_KsIHsD.1737879603072.2	mobile_web	645298964	215	view_listing	view_listing	614
-- 2025-01-25	--6fxqTzThxIoj0RtwDy8a4ugyUu.1737844586739.1	mobile_web	1819759706	323	view_listing	view_listing	383

select * from etsy-data-warehouse-prod.weblog.events where _date >= current_date-4 and visit_id in ('---aCiaj3WISnbLus7rgP_KsIHsD.1737879603072.2') and sequence_number >=  105
and event_type in ('view_listing','mix_and_match_v2_bundle_lp_shown') 
order by sequence_number asc
--only view listing events 


-- null: 
-- _date	visit_id	platform	listing_id	sequence_number	event_name	next_event	next_sequence_number
-- 2025-01-25	---CMyZLwv_NHoLBN1WGG4CHdwUN.1737774882789.2	mobile_web	1778816548	0	view_listing		
-- 2025-01-26	---EkaPC7_LOFWj8Jut2S-kR5Z65.1737928761247.2	mobile_web	797243458	0	view_listing		
-- 2025-01-26	---TGk7Lg4seatdufTPJK5fbRWn_.1737864403337.1	desktop	1786973379	572	view_listing		
select * from etsy-data-warehouse-prod.weblog.events where _date >= current_date-4 and visit_id in ('---CMyZLwv_NHoLBN1WGG4CHdwUN.1737774882789.2') 
and page_view = 1
-- and sequence_number >=  105
-- and event_type in ('view_listing','mix_and_match_v2_bundle_lp_shown') 
order by sequence_number asc
