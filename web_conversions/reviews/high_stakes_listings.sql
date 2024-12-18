
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
  _date >= current_date-2
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
	date(_partitiontime) >= current_date-2
	and beacon.event_name = "listing_page_reviews_seen"
group by all 
)
-- , reviews_and_views as (
select
  v.listing_type,
  v.listing_id,
  count(distinct v.visit_id) as unique_visits,
  sum(listing_views) as listing_views,
  sum(purchased_after_view) as purchases,
  sum(reviews_event_seen) as reviews_seen
from 
  views v
left join 
  seen_reviews r
    on v._date=r._date
    and v.visit_id=r.visit_id
    and v.listing_id=cast(r.listing_id as int64)
group by all
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
--------------------TESTNG TO SEE IF REVIEWS SEEN EVENT WORKS PROPERLY
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

--------------------TESTNG TO SEE IF REVIEWS + VIEWS MATCH UP PROPERLY 
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
  _date >= current_date-2
group by all 
)
select * from views where listing_id =  1737486091
-- _date	listing_id	visit_id	listing_type	listing_views	purchased_after_view
-- 2024-12-16	1737486091	qPReCCnSjYilsQLDYmHiMu5aUXko.1734374482054.1	low stakes	1	0
-- 2024-12-17	1737486091	oaGoGdbxrdicaXLeajxL7uGjNhSr.1734435160541.1	low stakes	1	0
-- 2024-12-16	1737486091	8bC0Wbj5QWmtgxmqCdldbw.1734385407829.1	low stakes	1	0


with seen_reviews as (
select
	date(_partitiontime) as _date,
	visit_id,
  regexp_extract(beacon.loc, r'listing/(\d+)') as listing_id,
  count(visit_id) as reviews_event_seen,
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
	date(_partitiontime) >= current_date-2
	and beacon.event_name = "listing_page_reviews_seen"
group by all 
)
select * from seen_reviews where listing_id in  ('1737486091')
-- _date	visit_id	listing_id	reviews_event_seen
-- 2024-12-17	oaGoGdbxrdicaXLeajxL7uGjNhSr.1734435160541.1	1737486091	1
-- 2024-12-16	qPReCCnSjYilsQLDYmHiMu5aUXko.1734374482054.1	1737486091	7

select * from etsy-data-warehouse-prod.weblog.events where visit_id in ('qPReCCnSjYilsQLDYmHiMu5aUXko.1734374482054.1') and event_type in ('listing_page_reviews_seen')
-- _date	run_date	visit_id	event_type	sequence_number	url	referrer	ref_tag	page_view	part_count	mobile_template	order_id	user_id	listing_id	listing_ids	is_preliminary	gdpr_p	gdpr_tp	epoch_ms
-- 2024-12-16	1734307200	qPReCCnSjYilsQLDYmHiMu5aUXko.1734374482054.1	listing_page_reviews_seen	264	http://www.etsy.com/listing/1737486091/18k-gold-filled-wide-bangle-gold?ga_order=most_relevant&ga_search_type=all&ga_view_type=gallery&ga_search_query=gold+bangles&ref=sc_gallery-1-9&pro=1&sts=1&plkey=8d5bf5ac3f5fbc1d1a98187d499a0dc659d3cd13%3A1737486091	https://www.etsy.com/search?q=gold%20bangles&ref=search_bar&explicit=1&max=15	sc_gallery-1-9	0	0	0					0	3	3	1734374624580
-- 2024-12-16	1734307200	qPReCCnSjYilsQLDYmHiMu5aUXko.1734374482054.1	listing_page_reviews_seen	257	http://www.etsy.com/listing/1737486091/18k-gold-filled-wide-bangle-gold?ga_order=most_relevant&ga_search_type=all&ga_view_type=gallery&ga_search_query=gold+bangles&ref=sc_gallery-1-9&pro=1&sts=1&plkey=8d5bf5ac3f5fbc1d1a98187d499a0dc659d3cd13%3A1737486091	https://www.etsy.com/search?q=gold%20bangles&ref=search_bar&explicit=1&max=15	sc_gallery-1-9	0	0	0					0	3	3	1734374621158
-- 2024-12-16	1734307200	qPReCCnSjYilsQLDYmHiMu5aUXko.1734374482054.1	listing_page_reviews_seen	274	http://www.etsy.com/listing/1737486091/18k-gold-filled-wide-bangle-gold?ga_order=most_relevant&ga_search_type=all&ga_view_type=gallery&ga_search_query=gold+bangles&ref=sc_gallery-1-9&pro=1&sts=1&plkey=8d5bf5ac3f5fbc1d1a98187d499a0dc659d3cd13%3A1737486091	https://www.etsy.com/search?q=gold%20bangles&ref=search_bar&explicit=1&max=15	sc_gallery-1-9	0	0	0					0	3	3	1734374627186
-- 2024-12-16	1734307200	qPReCCnSjYilsQLDYmHiMu5aUXko.1734374482054.1	listing_page_reviews_seen	266	http://www.etsy.com/listing/1737486091/18k-gold-filled-wide-bangle-gold?ga_order=most_relevant&ga_search_type=all&ga_view_type=gallery&ga_search_query=gold+bangles&ref=sc_gallery-1-9&pro=1&sts=1&plkey=8d5bf5ac3f5fbc1d1a98187d499a0dc659d3cd13%3A1737486091	https://www.etsy.com/search?q=gold%20bangles&ref=search_bar&explicit=1&max=15	sc_gallery-1-9	0	0	0					0	3	3	1734374624581
-- 2024-12-16	1734307200	qPReCCnSjYilsQLDYmHiMu5aUXko.1734374482054.1	listing_page_reviews_seen	224	http://www.etsy.com/listing/1737486091/18k-gold-filled-wide-bangle-gold?ga_order=most_relevant&ga_search_type=all&ga_view_type=gallery&ga_search_query=gold+bangles&ref=sc_gallery-1-9&pro=1&sts=1&plkey=8d5bf5ac3f5fbc1d1a98187d499a0dc659d3cd13%3A1737486091	https://www.etsy.com/search?q=gold%20bangles&ref=search_bar&explicit=1&max=15	sc_gallery-1-9	0	0	0					0	3	3	1734374588444
-- 2024-12-16	1734307200	qPReCCnSjYilsQLDYmHiMu5aUXko.1734374482054.1	listing_page_reviews_seen	256	http://www.etsy.com/listing/1737486091/18k-gold-filled-wide-bangle-gold?ga_order=most_relevant&ga_search_type=all&ga_view_type=gallery&ga_search_query=gold+bangles&ref=sc_gallery-1-9&pro=1&sts=1&plkey=8d5bf5ac3f5fbc1d1a98187d499a0dc659d3cd13%3A1737486091	https://www.etsy.com/search?q=gold%20bangles&ref=search_bar&explicit=1&max=15	sc_gallery-1-9	0	0	0					0	3	3	1734374621156
-- 2024-12-16	1734307200	qPReCCnSjYilsQLDYmHiMu5aUXko.1734374482054.1	listing_page_reviews_seen	272	http://www.etsy.com/listing/1737486091/18k-gold-filled-wide-bangle-gold?ga_order=most_relevant&ga_search_type=all&ga_view_type=gallery&ga_search_query=gold+bangles&ref=sc_gallery-1-9&pro=1&sts=1&plkey=8d5bf5ac3f5fbc1d1a98187d499a0dc659d3cd13%3A1737486091	https://www.etsy.com/search?q=gold%20bangles&ref=search_bar&explicit=1&max=15	sc_gallery-1-9	0	0	0					0	3	3	1734374627185
