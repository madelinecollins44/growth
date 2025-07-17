/* what % of shops with a listing view dont have any transactions */
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
