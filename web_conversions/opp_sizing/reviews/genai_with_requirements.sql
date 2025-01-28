--these are the only listings being considered. they active listings from from english language/ united states sellers.
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
-- text reviews that are in english
, reviews as (
select
  listing_id,
  count(transaction_id) as review_count,
	avg(((LENGTH(review) - LENGTH(replace(review, ' ', ''))) + 1)) as avg_review_length
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
),
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
	-- b.top_category,
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
  -- top_category,
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
  -- top_category,
	-- case
  -- 	when coalesce((p.price_usd/100), a.price_usd) > 100 then 'high stakes'
  -- 	else 'low stakes'
  -- end as listing_type,
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
-- left join 
--   etsy-data-warehouse-prod.listing_mart.listings p 
--     on cast(p.listing_id as string)=lv.listing_id
where a._date >=current_date-30
group by all
)
