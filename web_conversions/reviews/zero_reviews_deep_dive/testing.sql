------------------------------------------------------------------------------------------------------------------------------
-- what % of shops with a listing view dont have any transactions 
------------------------------------------------------------------------------------------------------------------------------
select
  count(distinct a.seller_user_id) as sellers_w_lv,
  count(distinct case when b.seller_user_id is null then a.seller_user_id end) as viewed_wo_trans
from 
   etsy-data-warehouse-prod.analytics.listing_views a
left join 
  etsy-data-warehouse-prod.transaction_mart.all_transactions b
    using (seller_user_id)
where  1=1
  and a._date >= current_date-30 
  and a.platform in ('mobile_web','desktop','boe')

------------------------------------------------------------------------------------------------------------------------------
-- spot checking shops without reviews or without transactions
------------------------------------------------------------------------------------------------------------------------------
with shops_wo_reviews as (
select 
  shop_id,
  seller_user_id,
  count(distinct transaction_id) as transactions,
  sum(has_review) as total_reviews
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
group by all 
)
select
  -- l.platform,
  l.seller_user_id,
  shop_name,
  case when total_reviews = 0 or r.seller_user_id is null then 0 else 1 end as has_shop_reviews,
  case when transactions = 0 or r.seller_user_id is null then 0 else 1 end as has_transactions,
  -- case when v.user_id is null or v.user_id= 0 then 0 else 1 end as signed_in,
  -- seller_user_id,
  count(distinct l.listing_id) as viewed_listings, 
  sum(purchased_after_view) as purchases,
  count(sequence_number) as views, 
from 
  etsy-data-warehouse-prod.analytics.listing_views l
left join 
  shops_wo_reviews r using (seller_user_id)
inner join 
  etsy-data-warehouse-prod.weblog.visits v
      on l.visit_id=v.visit_id
inner join 
  etsy-data-warehouse-prod.rollups.seller_basics sb 
    on sb.user_id=l.seller_user_id
where 
  l._date >= current_date-30 
  and v._date >= current_date-30 
  and (total_reviews = 0 or r.seller_user_id is null)
  and transactions = 1
group by all 
order by 7 desc
limit 10
/* 
--- NO REVIEWS


--- NO TRANSACTIONS
seller_user_id	shop_name	has_shop_reviews	has_transactions	viewed_listings	purchases	views
1088175042	InsightByDaniel	0	0	7	0	135072
912453789	DRHALS999	0	0	1	0	93682
972158091	DesignWizardCreation	0	0	1	0	42415
1083323145	PeachesandMandarines	0	0	14	0	39387
1086693596	WordByDeshdeepak	0	0	20	0	38015
1082376266	SaveGet	0	0	45	0	32486
*/

--checking to make sure listing views match
select count(sequence_number) from etsy-data-warehouse-prod.analytics.listing_views where seller_user_id =1086693596 and _date >= current_date-30
