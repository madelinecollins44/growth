with review_count as (
select
    review, 
    listing_id, 
    count(*) AS cnt
  from 
    `etsy-data-warehouse-prod.etsy_shard.shop_transaction_review`  
  where
  review <> ''
  and review is not null 
  and is_deleted = 0 -- not deleted 
group by all 
having count(*) > 1
) 
, duped_listings as (
select distinct
  listing_id
from 
  review_count
)
, lv_stats as (
select
  listing_id,
  count(sequence_number) as views,
  sum(purchased_after_view) as purchases 
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all 
)
, listing_gms as ( 
select
	t.listing_id, 
	sum(tg.trans_gms_net) as gms_net
from
	`etsy-data-warehouse-prod`.transaction_mart.transactions_visits tv
join
	`etsy-data-warehouse-prod`.transaction_mart.transactions_gms_by_trans tg
using(transaction_id)
join
	`etsy-data-warehouse-prod`.transaction_mart.all_transactions t
on
	tv.transaction_id = t.transaction_id
where
	tv.date >= current_date-365
	and tv.platform_app in ('mobile_web','desktop')
group by all 
)
select
  count(distinct lvs.listing_id) as viewed_listings,
  count(distinct dl.listing_id) as duped_viewed_listings,
  sum(views) as total_lv,
  sum(case when dl.listing_id is not null then views end) as duped_lv,
  sum(purchases) as total_purchases,
  sum(case when dl.listing_id is not null then purchases end) as duped_purchases,
  sum(gms_net) as gms,
  sum(case when dl.listing_id is not null then gms_net end) as duped_gms
from 
  lv_stats lvs
left join 
  listing_gms gms 
    using (listing_id)
left join 
  duped_listings dl 
    on lvs.listing_id=dl.listing_id
  
