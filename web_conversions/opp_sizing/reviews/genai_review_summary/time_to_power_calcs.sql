--these are the only listings being considered. they active listings from from english language/ united states sellers.these listings are not blocklisted. 
with active_english_listings as (
select
  alb.listing_id,
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
from  
  active_english_listings
inner join 
  etsy-data-warehouse-prod.rollups.transaction_reviews using (listing_id)
where 
  has_text_review > 0  
  and language in ('en')
group by all
having count(transaction_id) >= 5 and count(transaction_id) <= 100
order by 2 desc
)
, listing_views as (
select
  platform,
	listing_id,
  visit_id,
	count(visit_id) as listing_views,	
  sum(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.analytics.listing_views a
where 
  _date >=current_date-30
  and platform in ('mobile_web','desktop')
group by all
)
select
  platform,
  count(distinct lv.listing_id) as unique_listings,
  sum(listing_views) as listing_views,
  count(distinct lv.visit_id) as unique_visits,
  sum(purchases) as purchases
from 
  listing_views lv
inner join 
  reviews 
    on lv.listing_id=reviews.listing_id
group by all
