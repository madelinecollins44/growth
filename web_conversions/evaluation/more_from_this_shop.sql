--------------------------------------------------------------------------------
-- WHAT % OF LISTING VIEWS SEE THE 'MORE FROM THIS SHOP' MODULE? 
--------------------------------------------------------------------------------
-- MFTS seen events 
inner join 
  etsy-data-warehouse-prod.weblog.visits using (visit_id)
where
	date(_partitiontime) >= current_date-30
  and _date >= current_date-30
	and (beacon.event_name in ("view_listing") -- listing views 
      or (beacon.event_name in ("recommendations_module_seen") and (select value from unnest(beacon.properties.key_value) where key = "module_placement") in ("listing_side"))) -- MFTS modules 
  and platform in ('mobile_web','desktop')

-- listing views from the MFTS module (clicks on module)
select
  count(distinct visit_id) as visits,
  count(sequence_number) as listing_views
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  platform in ('mobile_web','desktop')
  and ref_tag like ('related%')
  and _date >= current_date-30

--------------------------------------------------------------------------------
-- LOOKING AT LISTING VIEWS THAT THEN SAW MFTS MODULE (using lead functions)
--------------------------------------------------------------------------------
with listing_events as (
select
	date(_partitiontime) as _date,
	visit_id,
  platform,
  sequence_number,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), (select value from unnest(beacon.properties.key_value) where key = "target_listing_id")) as listing_id, 
	case
    when beacon.event_name in ('view_listing') then 'view_listing'
    when beacon.event_name in ('recommendations_module_seen') then 'mfts_module'
  end as event_name,
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
inner join 
  etsy-data-warehouse-prod.weblog.visits using (visit_id)
where
	date(_partitiontime) >= current_date-30
  and _date >= current_date-30
	and (beacon.event_name in ("view_listing") -- listing views 
      or (beacon.event_name in ("recommendations_module_seen") and (select value from unnest(beacon.properties.key_value) where key = "module_placement") in ("listing_side"))) -- MFTS modules 
  and platform in ('mobile_web','desktop')
group by all 
)
, ordered_events as (
select
	_date,
	visit_id,
	listing_id,
  platform,
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
	listing_id,
  platform,
  sequence_number,
	case when next_event in ('mfts_module') then 1 else 0 end as saw_mfts_module
from
	ordered_events
where 
	event_name in ('view_listing')
)
select
  lv.platform,
	case when a.listing_id is null then 1 else 0 end as missing_in_analytics,
	count(lv.visit_id) as listing_views,
	count(a.visit_id) as a_listing_views,
	count(case when saw_mfts_module = 1 then lv.visit_id end) as views_and_mfts_seen,
	sum(case when saw_mfts_module = 1 then purchased_after_view end) as saw_mfts_and_purchased,
	sum(purchased_after_view) as purchases
from 
	listing_views lv
left join 
	etsy-data-warehouse-prod.analytics.listing_views a
		on lv.listing_id=cast(a.listing_id as string)
		and lv.visit_id=a.visit_id
		and lv.sequence_number=a.sequence_number	
where a._date >=current_date-30
group by all

--------------------------------------------------------------------------------
-- TESTING
--------------------------------------------------------------------------------
/* TEST 1: see if target_listing_id or page_listing_id is better to extract listing_id from MFTS module */
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
-- mfts_modules_seen	target_listing_id	page_listing_id
-- 34234772	34234772	34234772
  and _date >= current_date-30
	and (beacon.event_name in ("recommendations_module_seen") and (select value from unnest(beacon.properties.key_value) where key = "module_placement") in ("listing_side")) -- MFTS modules 
  and platform in ('mobile_web','desktop')
group by all 
