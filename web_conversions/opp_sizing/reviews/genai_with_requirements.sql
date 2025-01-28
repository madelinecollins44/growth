--these are the only listings being considered. they active listings from from english language/ united states sellers.
with active_english_listings as (
select
  distinct listing_id
from 
  etsy-data-warehouse-prod.rollups.active_listing_basics
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics using (shop_id)
where 
  active_seller_status=1 -- active sellers 
  and primary_language in ('en-US') -- only shops with english/ us as primary language 
  and country_name in ('United States') -- only US sellers 
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
  active_english_listings 
	`etsy-data-warehouse-prod`.transaction_mart.transactions_visits tv -- only looking for mweb, desktop visits 
inner join
	`etsy-data-warehouse-prod`.transaction_mart.transactions_gms_by_trans tg using(transaction_id) -- need gms 
inner join
	`etsy-data-warehouse-prod`.transaction_mart.all_transactions t on tv.transaction_id = t.transaction_id -- need listing_id
where
	(tv.mapped_platform_type in ('desktop') or tv.mapped_platform_type like ('mweb%')) -- only gms from web transactions 
	and t.date >= current_date - 30
group by all 
)
-- active listings from us-en shops 
, english_shops as (
select
  distinct shop_id
from 
  etsy-data-warehouse-prod.rollups.seller_basics
where 
  active_seller_status=1 -- active sellers 
  and primary_language in ('en-US') -- only shops with english/ us as primary language 
)
