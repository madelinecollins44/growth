-----------------------------------------------------------------------------------------------
--looking at reviews x listing views/ gms coverage to understand what threshold we need to use
-----------------------------------------------------------------------------------------------
-- create or replace table etsy-data-warehouse-dev.madelinecollins.genai_listings_opp_size as (
-- with web_gms as (
-- select
--   listing_id,
--   sum(trans_gms_net) as gms_net
-- from
-- 	`etsy-data-warehouse-prod`.transaction_mart.transactions_visits tv -- only looking for mweb, desktop visits 
-- inner join
-- 	`etsy-data-warehouse-prod`.transaction_mart.transactions_gms_by_trans tg using(transaction_id) -- need gms 
-- inner join
-- 	`etsy-data-warehouse-prod`.transaction_mart.all_transactions t on tv.transaction_id = t.transaction_id -- need listing_id
-- where
-- 	(tv.mapped_platform_type in ('desktop') or tv.mapped_platform_type like ('mweb%')) -- only gms from web transactions 
-- 	and t.date >= current_date - 365
-- group by all 
-- )
-- , listing_views as (
-- select
--   a.listing_id,
--   -- case
--   -- 	when coalesce((p.price_usd/100), a.price_usd) > 100 then 'high stakes'
--   -- 	else 'low stakes'
--   -- end as listing_type,
--   -- b.top_category,
--   count(a.visit_id) as listing_views,
--   sum(a.purchased_after_view) as purchases
-- from 
--   etsy-data-warehouse-prod.rollups.active_listing_basics b
-- left join 
--   etsy-data-warehouse-prod.listing_mart.listings p using (listing_id)
-- left join  
--     etsy-data-warehouse-prod.analytics.listing_views a
--       on a.listing_id=b.listing_id
-- where 
--   _date >= current_date-30
--   and a.platform in ('mobile_web','desktop') -- only web lv 
-- group by all 
-- )
-- , reviews as (
-- select
--   listing_id,
--   count(transaction_id) as review_count
-- from  
--   etsy-data-warehouse-prod.rollups.transaction_reviews
-- where 
--   has_review > 0  
--   and language in ('en')
-- group by all
-- order by 2 desc
-- )
-- select
--   lv.listing_id,
--   -- listing_type,
--   -- top_category,
--   review_count as reviews,
--   sum(listing_views) as listing_views,
--   sum(purchases) as purchases,
--   sum(gms_net) as gms_net,
-- from 
--   listing_views lv
-- left join 
--   reviews 
--     on lv.listing_id=reviews.listing_id
-- left join 
--   web_gms on lv.listing_id=web_gms.listing_id
-- group by all
-- );

--adding in top category + price 
create or replace table etsy-data-warehouse-dev.madelinecollins.genai_category_highstakes_listings_opp_size as (
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
	and beacon.event_name in ("listing_page_reviews_seen","view_listing")
  and platform in ('mobile_web','desktop')
group by all 
)
, ordered_events as (
select
	_date,
	visit_id,
	a.listing_id,
	b.top_category,
  sequence_number,
	event_name,
	lead(event_name) over (partition by visit_id, a.listing_id order by sequence_number) as next_event,
	lead(sequence_number) over (partition by visit_id, a.listing_id order by sequence_number) as next_sequence_number
from 
	listing_events a 
inner join   
  etsy-data-warehouse-prod.rollups.active_listing_basics b  
    on cast(a.listing_id as int64)=b.listing_id
)
, listing_views as (
select
	visit_id,
	listing_id,
  top_category,
  sequence_number,
	case when next_event in ('listing_page_reviews_seen') then 1 else 0 end as saw_reviews
from
	ordered_events
where 
	event_name in ('view_listing')
)
, listing_views_and_reviews_seen as (
select
	cast(lv.listing_id as int64) as listing_id,
  top_category,
	case
  	when coalesce((p.price_usd/100), a.price_usd) > 100 then 'high stakes'
  	else 'low stakes'
  end as listing_type,
	count(lv.visit_id) as listing_view_count,	
  sum(purchased_after_view) as purchases,
	count(case when saw_reviews = 1 then lv.visit_id end) as views_and_reviews_seen,
	sum(case when saw_reviews = 1 then purchased_after_view end) as saw_reviews_and_purchased,
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
, reviews as (
select
  listing_id,
  count(transaction_id) as review_count,
	avg(((LENGTH(review) - LENGTH(replace(review, ' ', ''))) + 1)) as avg_review_length
from  
  etsy-data-warehouse-prod.rollups.transaction_reviews
where 
  has_text_review > 0  
  and language in ('en')
group by all
order by 2 desc
)
, web_gms as (
select
  listing_id,
  sum(trans_gms_net) as gms_net
from
	`etsy-data-warehouse-prod`.transaction_mart.transactions_visits tv -- only looking for mweb, desktop visits 
inner join
	`etsy-data-warehouse-prod`.transaction_mart.transactions_gms_by_trans tg using(transaction_id) -- need gms 
inner join
	`etsy-data-warehouse-prod`.transaction_mart.all_transactions t on tv.transaction_id = t.transaction_id -- need listing_id
where
	(tv.mapped_platform_type in ('desktop') or tv.mapped_platform_type like ('mweb%')) -- only gms from web transactions 
	and t.date >= current_date - 365
group by all 
)
select
  lv.listing_id,
  top_category,
	listing_type,
  review_count as text_reviews,
	avg_review_length, 
  sum(listing_view_count) as listing_views,
  sum(purchases) as purchases,
  sum(views_and_reviews_seen) as views_and_reviews_seen,
	sum(saw_reviews_and_purchased) as saw_reviews_and_purchased,
  sum(gms_net) as gms_net,
from 
  listing_views_and_reviews_seen lv
left join 
  reviews 
    on lv.listing_id=reviews.listing_id
left join 
  web_gms on lv.listing_id=web_gms.listing_id
group by all
);

-------------------------------------------------------------------------------
-- testing
-------------------------------------------------------------------------------
-- TESTING reviews seen event 
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
	and beacon.event_name in ("listing_page_reviews_seen","view_listing")
  and platform in ('mobile_web','desktop')
group by all 
)
, ordered_events as (
select
	_date,
	visit_id,
	a.listing_id,
	b.top_category,
  sequence_number,
	event_name,
	lead(event_name) over (partition by visit_id, a.listing_id order by sequence_number) as next_event,
	lead(sequence_number) over (partition by visit_id, a.listing_id order by sequence_number) as next_sequence_number
from 
	listing_events a 
inner join   
  etsy-data-warehouse-prod.rollups.active_listing_basics b  
    on cast(a.listing_id as int64)=b.listing_id
)
, listing_views as (
select
	visit_id,
	listing_id,
  sequence_number,
	case when next_event in ('listing_page_reviews_seen') then 1 else 0 end as saw_reviews
from
	ordered_events
where 
	event_name in ('view_listing')
)
select listing_id, count(visit_id) from listing_views where listing_id in ('1167562350','667282692','1010384443') group by all 
-- TESTING make sure total counts match up w what im seeing
select  
  count(visit_id) as views,
  sum(purchased_after_view) as purchases
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics b 
inner join 
  etsy-data-warehouse-prod.analytics.listing_views lv using (listing_id) -- only looking at viewed listings 
where
  lv._date >= current_date-30
  and lv.platform in ('mobile_web','desktop')
group by all 
-- views	purchases
-- 1151932286	19252286
-- looking at all web purchases over last 365 days 

with web_gms as (
select
  listing_id,
  sum(trans_gms_net) as gms_net
from
	`etsy-data-warehouse-prod`.transaction_mart.transactions_visits tv -- only looking for mweb, desktop visits 
inner join
	`etsy-data-warehouse-prod`.transaction_mart.transactions_gms_by_trans tg using(transaction_id) -- need gms 
inner join
	`etsy-data-warehouse-prod`.transaction_mart.all_transactions t on tv.transaction_id = t.transaction_id -- need listing_id
where
	(tv.mapped_platform_type in ('desktop') or tv.mapped_platform_type like ('mweb%')) -- only gms from web transactions 
	and t.date >= current_date - 365
group by all 
)
-- only gms from listings that are still active
select
  sum(gms_net)
from web_gms
inner join etsy-data-warehouse-prod.rollups.active_listing_basics using (listing_id)
--4537400362.9
  
--TESTING to make sure # of listings match with the query 
with reviews as (
select
 listing_id,
 sum(has_review) as review_count
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
where
  language in ('en')
group by all
having sum(has_review) >= 100
)
, active_listing_views as (
select
  listing_id,
  count(visit_id) as views,
  sum(purchased_after_view) as purchases
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics b 
inner join 
  etsy-data-warehouse-prod.analytics.listing_views lv using (listing_id) -- only looking at viewed listings 
where
  lv._date >= current_date-30
  and lv.platform in ('mobile_web','desktop')
group by all 
)
select
  count(distinct lv.listing_id) as total_listings,
  sum(views) as views,
  sum(purchases) as purchases
from 
  active_listing_views lv
inner join 
 reviews r on lv.listing_id=r.listing_id
where r.review_count >= 100
