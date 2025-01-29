-----------------------------------------------------------------------------------
-- table created to collect all eligible listings 
-----------------------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.genai_category_highstakes_listings_opp_size as (
--these are the only listings being considered. they active listings from from english language/ united states sellers.these listings are not blocklisted. 
with active_english_listings as (
select
  alb.listing_id,
  top_category
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics alb
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics sb using (shop_id)
left join 
  (select 
    distinct listing_id 
  from 
    etsy-data-warehouse-prod.integrations.blocklisted_listings
  where 
    _date >= current_date-1095) bl -- any listings that have been blocked over the last 3 years 
      on alb.listing_id = bl.listing_id
where 
  active_seller_status=1 -- active sellers 
  and primary_language in ('en-US') -- only shops with english/ us as primary language 
  and sb.country_name in ('United States') -- only US sellers 
  and bl.listing_id is null -- excluding blocked listings
)
-- text reviews that are in english
, reviews as (
select
  listing_id,
  count(transaction_id) as review_count,
	round(avg(((LENGTH(review) - LENGTH(replace(review, ' ', ''))) + 1)),1) as avg_review_length,
  round(sum(((LENGTH(review) - LENGTH(replace(review, ' ', ''))) + 1)),1) as total_word_count
from  
  active_english_listings
inner join 
  etsy-data-warehouse-prod.rollups.transaction_reviews using (listing_id)
where 
  has_text_review > 0  
  and language in ('en')
group by all
order by 2 desc
)
-- gms from active listings over the last 30 days from web sources
, web_gms as (
select
  listing_id,
  sum(trans_gms_net) as gms_net
from
  active_english_listings -- only eligibile listings
inner join 
  `etsy-data-warehouse-prod`.transaction_mart.all_transactions t using (listing_id) -- gets transaction_id using listing_id
inner join
	`etsy-data-warehouse-prod`.transaction_mart.transactions_gms_by_trans gms -- pulls gms from transaction_id
    on t.transaction_id=gms.transaction_id
inner join 
  `etsy-data-warehouse-prod`.transaction_mart.transactions_visits tv -- only looking for mweb, desktop visits 
    on tv.transaction_id=gms.transaction_id
where
	(tv.mapped_platform_type in ('desktop') or tv.mapped_platform_type like ('mweb%')) -- only gms from web transactions 
	and t.date >= current_date - 30
group by all 
)
-- now, can pull in view_listing + reviews_seen data for eligible listings on web 
, listing_events as (
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
  and platform in ('mobile_web','desktop') -- only web visits 
group by all 
)
, ordered_events as (
select
	_date,
	visit_id,
	a.listing_id,
  top_category,
  sequence_number,
	event_name,
	lead(event_name) over (partition by visit_id, a.listing_id order by sequence_number) as next_event,
	lead(sequence_number) over (partition by visit_id, a.listing_id order by sequence_number) as next_sequence_number
from 
	listing_events a 
inner join   
  active_english_listings b -- only looking at eligible listings 
    on a.listing_id=cast(b.listing_id as string)
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
select
  lv.listing_id,
  top_category,
	listing_type,
  review_count as text_reviews,
	avg_review_length, 
  total_word_count,
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
order by review_count desc
);

-------------------------------------------------------------------------------
-- testing
-------------------------------------------------------------------------------
-- TEST 1: how many listings are eligible? 
select
  count(distinct listing_id) as eligibile_listings,
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics alb
-- inner join 
--   etsy-data-warehouse-prod.rollups.seller_basics sb using (shop_id)
-- where 
--   active_seller_status=1 -- active sellers 
--   and primary_language in ('en-US') -- only shops with english/ us as primary language 
--   and sb.country_name in ('United States') -- only US sellers 
------ 68016975 eligibile_listings
------ 125494691 active listings 
------ 54.2% of active listings are eligible for this opp sizing

	
--  TEST 2: the the listing view/ purchase count correct when aggregated? 
with active_english_listings as (
select
  listing_id,
  top_category
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics alb
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics sb using (shop_id)
where 
  active_seller_status=1 -- active sellers 
  and primary_language in ('en-US') -- only shops with english/ us as primary language 
  and sb.country_name in ('United States') -- only US sellers 
)
select
  count(distinct lv.listing_id) as listings_viewed,
  count(visit_id) as views,
  sum(purchased_after_view) as purchases
from 
  active_english_listings
inner join 
  etsy-data-warehouse-prod.analytics.listing_views lv using (listing_id) -- only looking at viewed listings 
where
  lv._date >= current_date-30
  and lv.platform in ('mobile_web','desktop')
group by all 
------ 32096964 eligible listings have been viewed over the last 30 days
------ 496939192 listing views
------ 8237292 purchases  
------ 1.7% is current conversion rate on web

-- TEST 3: does the aggregated gms make sense when were looking at it separately?
with active_english_listings as (
select
  listing_id,
  top_category
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics alb
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics sb using (shop_id)
where 
  active_seller_status=1 -- active sellers 
  and primary_language in ('en-US') -- only shops with english/ us as primary language 
  and sb.country_name in ('United States') -- only US sellers 
)
select
  sum(trans_gms_net) as gms_net
from
  active_english_listings -- only eligibile listings
inner join 
  `etsy-data-warehouse-prod`.transaction_mart.all_transactions t using (listing_id) -- gets transaction_id using listing_id
inner join
	`etsy-data-warehouse-prod`.transaction_mart.transactions_gms_by_trans gms -- pulls gms from transaction_id
    on t.transaction_id=gms.transaction_id
inner join 
  `etsy-data-warehouse-prod`.transaction_mart.transactions_visits tv -- only looking for mweb, desktop visits 
    on tv.transaction_id=gms.transaction_id
where
	(tv.mapped_platform_type in ('desktop') or tv.mapped_platform_type like ('mweb%')) -- only gms from web transactions 
	and t.date >= current_date - 30
group by all 
------ 175630296.94 is the total gms from these listings over the last 30 days

-- TEST 4: reviews seen event 
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
), active_english_listings as (
select
  listing_id,
  top_category
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics alb
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics sb using (shop_id)
where 
  active_seller_status=1 -- active sellers 
  and primary_language in ('en-US') -- only shops with english/ us as primary language 
  and sb.country_name in ('United States') -- only US sellers 
)
select 
  event_name,
  count(visit_id) as views
from active_english_listings ael
inner join listing_events le
  on cast(ael.listing_id as string)=le.listing_id
group by all
------ 497001247 eligible listings have been viewed over the last 30 days, this matches count from analytics.listing_views by about 99% 
------ 131708147 review_seen events -- this is higher than whats in the sheet, bc we need to look at review_seen events that happen post listing view
------ testing to check lead functions: https://github.com/madelinecollins44/growth/blob/8db448026ef8d324381d0edfa5cd63cb95588f23/web_conversions/reviews/high_stakes_listings_lead.sql#L298

-- TEST 5: what is the % of listing views that also scroll to reviews? in theory, this % should match up to the difference in review_seen event count above and the total in sheet. 
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
), active_english_listings as (
select
  listing_id,
  top_category
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics alb
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics sb using (shop_id)
where 
  active_seller_status=1 -- active sellers 
  and primary_language in ('en-US') -- only shops with english/ us as primary language 
  and sb.country_name in ('United States') -- only US sellers 
), ordered_events as (
select
	_date,
	visit_id,
	a.listing_id,
  top_category,
  sequence_number,
	event_name,
	lead(event_name) over (partition by visit_id, a.listing_id order by sequence_number) as next_event,
	lead(sequence_number) over (partition by visit_id, a.listing_id order by sequence_number) as next_sequence_number
from 
	listing_events a 
inner join   
  active_english_listings b -- only looking at eligible listings 
    on a.listing_id=cast(b.listing_id as string)
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
select 
	count(visit_id) as lv_events, 
	count(case when saw_reviews > 0 then visit_id end) as rs_events 
from listing_views
------ 497001247 lv
------ 107561099 lv + review seen events 
------ the % of visits with a listing view + review seen event is 21.6%, which covers the 86%ish difference between original review seen counts and what was on the sheet. 

-- TEST 6: make sure total_word_count is working correctly
with active_english_listings as (
select
  listing_id,
  top_category
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics alb
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics sb using (shop_id)
where 
  active_seller_status=1 -- active sellers 
  and primary_language in ('en-US') -- only shops with english/ us as primary language 
  and sb.country_name in ('United States') -- only US sellers 
)
select
  listing_id,
  review,
  count(transaction_id) as review_count,
	avg(((LENGTH(review) - LENGTH(replace(review, ' ', ''))) + 1)) as avg_review_length,
  sum(((LENGTH(review) - LENGTH(replace(review, ' ', ''))) + 1)) as total_word_count
from  
  active_english_listings
inner join 
  etsy-data-warehouse-prod.rollups.transaction_reviews using (listing_id)
where 
  has_text_review > 0  
  and language in ('en')
  and listing_id in (881491068)
group by all
order by 2 desc
------ averages matches with total_word_count

select distinct listing_id from etsy-data-warehouse-dev.madelinecollins.genai_category_highstakes_listings_opp_size where total_word_count = 40 limit 5
-- listing_id
-- 999645422
-- 1813184097
-- 1526833396
-- 886323951
-- 1755143837

select * from etsy-data-warehouse-prod.rollups.transaction_reviews where listing_id = 1813184097 and has_text_review >0 and language in ('en')

-- TEST 6: make sure blocklisted listings are actually excluded
select
  alb.listing_id,
  top_category
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics alb
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics sb using (shop_id)
left join 
  etsy-data-warehouse-prod.integrations.blocklisted_listings bl
    on alb.listing_id = bl.listing_id
where 
  active_seller_status=1 -- active sellers 
  and primary_language in ('en-US') -- only shops with english/ us as primary language 
  and sb.country_name in ('United States') -- only US sellers 
  and bl.listing_id is null -- excluding blocked listings
  and bl._date >= current_date-1095
)
select * from active_english_listings where listing_id in (117727208, 116315503, 115496790, 116686403, 117655652, 113235360)

-----grabbing listing_id for blocklisted active listings
-- select 
--   distinct alb.listing_id 
-- from 
--   etsy-data-warehouse-prod.rollups.active_listing_basics alb
-- inner join 
--   etsy-data-warehouse-prod.integrations.blocklisted_listings bl
--     on alb.listing_id = bl.listing_id
-- where bl._date >= current_date-730
--   limit 10 

--   listing_id
-- 117727208
-- 116315503
-- 115496790
-- 116686403
-- 117655652
-- 113235360
-- 89946785
-- 78571731
-- 82236898
-- 82485648
--these are the only listings being considered. they active listings from from english language/ united states sellers.these listings are not blocklisted. 
-- with active_english_listings as (
select
  count(distinct alb.listing_id) as active_listings,
  count(distinct bl.listing_id) as blocklisted_listings
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics alb
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics sb using (shop_id)
left join 
  (select 
    distinct listing_id 
  from 
    etsy-data-warehouse-prod.integrations.blocklisted_listings
  where 
    _date >= current_date-1095) bl -- any listings that have been blocked over the last 3 years 
      on alb.listing_id = bl.listing_id
where 
  active_seller_status=1 -- active sellers 
  and primary_language in ('en-US') -- only shops with english/ us as primary language 
  and sb.country_name in ('United States') -- only US sellers 
  -- and bl.listing_id is null -- excluding blocked listings
-- )

-- active_listings	blocklisted_listings
-- 68008612	8820068
-- select 8820068/68008612
------0.12969045743794919 , about 13% of active listings viewed are blocklisted
