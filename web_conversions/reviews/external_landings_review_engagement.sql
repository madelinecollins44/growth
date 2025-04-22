with listing_events as (
select
	date(_partitiontime) as _date,
	visit_id,
  platform,
  sequence_number,
	beacon.event_name as event_name,
  case when beacon.loc like ('%external=1%') then 1 else 0 end as external_view,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
inner join 
  etsy-data-warehouse-prod.weblog.visits using (visit_id)
where
	date(_partitiontime) >= current_date-30
  and _date >= current_date-30
	and beacon.event_name in ("listing_page_reviews_seen","view_listing")
  and platform in ('mobile_web','desktop')
	-- and beacon.event_source in ('web')
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
  external_view,
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
  external_view,
	case when next_event in ('listing_page_reviews_seen') then 1 else 0 end as saw_reviews
from
	ordered_events
where 
	event_name in ('view_listing')
)
, lv_stats as (
select
  lv.platform,
	lv.listing_id,
  lv.external_view,
	case when a.listing_id is null then 1 else 0 end as missing_in_analytics,
	count(lv.visit_id) as listing_views,
	count(a.visit_id) as a_listing_views,
	count(case when saw_reviews = 1 then lv.visit_id end) as views_and_reviews_seen,
	sum(case when saw_reviews = 1 then purchased_after_view end) as saw_reviews_and_purchased,
	sum(purchased_after_view) as purchases
from 
	listing_views lv
left join 
	etsy-data-warehouse-prod.analytics.listing_views a
		on lv.listing_id=cast(a.listing_id as string)
		and lv.visit_id=a.visit_id
		and lv.sequence_number=a.sequence_number	
left join 
  etsy-data-warehouse-prod.listing_mart.listings p 
    on cast(p.listing_id as string)=lv.listing_id
where a._date >=current_date-30
group by all
)
select
  platform,
  external_view,
	sum(missing_in_analytics) as errors,
  count(distinct rv.listing_id) as listings_viewed,
  sum(rv.listing_views) as listing_views,
  sum(rv.purchases) as purchases,
  sum(rv.saw_reviews_and_purchased) as saw_reviews_and_purchased,
  --listing page 
  sum(rv.views_and_reviews_seen) as views_and_reviews_seen,
from
  lv_stats rv
group by all

	
-- GRAB % OF EXTERNAL LISTING VIEWS FROM EACH PARAM
select 
  case 
    when url like ('%external=1%')  then 'external=1'
    when url like ('%gpla=1%') then 'gpla=1'
    else 'internal'
  end as listing_type,
  count(lv.sequence_number) as lv, 
  sum(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.weblog.events e
inner join 
  etsy-data-warehouse-prod.analytics.listing_views lv 
    on e.visit_id=lv.visit_id
    and e.sequence_number=lv.sequence_number
    and e.listing_id= cast(lv.listing_id as string)
where 
  event_type in ('view_listing') 
  and lv._date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all
------------------------------------------------------------
-- TESTING
------------------------------------------------------------
-- TEST 1: make sure listing views match up from analytics 
select 
  case when url like ('%external=1%') then 1 else 0 end as external_listing,
  count(lv.sequence_number) as lv, 
  sum(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.weblog.events e
inner join 
  etsy-data-warehouse-prod.analytics.listing_views lv 
    on e.visit_id=lv.visit_id
    and e.sequence_number=lv.sequence_number
    and e.listing_id= cast(lv.listing_id as string)
where 
  event_type in ('view_listing') 
  and lv._date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all
/*external_listing	lv	purchases
0	1089720954	21960013
1	54208698	501330
	*/
	
select 
  count(lv.sequence_number) as lv, 
  sum(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.analytics.listing_views lv 
where 
  lv._date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all
/* lv	purchases
1143929652	22461343
	*/

--- TEST 2: see what the external urls look like 
with agg as (
select 
  case when url like ('%external=1%') then 1 else 0 end as external_listing,
  url,
  count(lv.sequence_number) as lv, 
  sum(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.weblog.events e
inner join 
  etsy-data-warehouse-prod.analytics.listing_views lv 
    on e.visit_id=lv.visit_id
    and e.sequence_number=lv.sequence_number
    and e.listing_id= cast(lv.listing_id as string)
where 
  event_type in ('view_listing') 
  and lv._date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all
)
-- select distinct url from agg where external_listing > 0 limit 10 
select distinct url from agg where external_listing = 0 limit 10 

/*url
http://www.etsy.com/de-en/listing/1830721073/authentic-haitian-chocolate-cocoa-balls?external=1&ref=landingpage_similar_listing_top-2&plkey=05d86ee97c02b0bee1bdba745a168b1c839b071f%3A1830721073
http://www.etsy.com/es/listing/192544434/crackle-white-1006-frasco-de-esmalte?external=1&ref=landingpage_similar_listing_top-1&sts=1&logging_key=d1239cc07a5a1fcc96b2faa8e1c5556dae46ef56%3A192544434
http://www.etsy.com/uk/listing/1778052489/50pcs-8x8x35cm-jewelry-box-custom-logo?external=1&ref=landingpage_similar_listing_top-1&frs=1&sts=1&plkey=ddeb62b714cb1396dfc222d892ffbd70982b2dbd%3A1778052489
http://www.etsy.com/fr/listing/1651129584/art-mural-vintage-pour-chambre-denfant?ls=a&external=1&ref=pla_similar_listing_top-2&pro=1&sts=1&plkey=23133b783843756e1ea9459297b61c08bf4030e7%3A1651129584
http://www.etsy.com/listing/1899773365/vintage-cor-alpha-late-1970s-five-piece?external=1&rec_type=cs&ref=landingpage_similar_listing_top-6&frs=1&logging_key=069769263a28cb0a4654a83f2b93f5415744e427%3A1899773365
http://www.etsy.com/listing/1182143586/cho-iran-nabashad-persian-calligraphy?ls=r&external=1&rec_type=ss&ref=pla_similar_listing_top-1&pro=1&frs=1&content_source=8c46001be44b2d52519438a3f7a4b892d28b08a0%253A1182143586&logging_key=8c46001be44b2d52519438a3f7a4b892d28b08a0%3A1182143586
http://www.etsy.com/ca/listing/1863693983/psalm-23-wall-art-christian-home-decor?external=1&ref=pla_similar_listing_top-1&pro=1&frs=1&plkey=26752735e9ba1fe62b68f05fbd8f86f8981d1fd7%3A1863693983
http://www.etsy.com/listing/1406381645/7x11-anatolian-rughandmade-rugoushak?external=1&ref=landingpage_similar_listing_top-2&pro=1&frs=1&sts=1&plkey=ca4160cc1492f009aec4d8f51808cbf9f0999840%3A1406381645
http://www.etsy.com/listing/910893586/traditional-japanese-hannya-mask-oni?external=1&rec_type=ss&ref=landingpage_similar_listing_top-1&pro=1&logging_key=2dafb1078a42f07e6f17e8fd2d580bfc94204378%3A910893586
http://www.etsy.com/it/listing/1469218654/bordi-da-giardino-spessi-in-bambu?external=1&rec_type=ss&ref=pla_similar_listing_top-1&logging_key=e3dc258bd4c507ced8a31887b8f9cf4727fd3080%3A1469218654 */
