with trans as (
select
  a.seller_user_id,
  a.listing_id,
  sum(trans_gms_net) as gms_net,
  count(distinct a.transaction_id) as transactions
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans g
inner join  
  etsy-data-warehouse-prod.transaction_mart.all_transactions a using (transaction_id)
where 1=1 
  and a.date between date('2025-07-01') and date('2025-07-08') -- purchases during the experiment 
group by all 
)
, tenure as (
select
  user_id,
  create_date,
  date_diff(current_date(), create_date, month) as months_on_etsy,
    case
      when date_diff(current_date(), create_date, day) < 30 then 'New'
      when date_diff(current_date(), create_date, month) = 1 then '1 month'
      when date_diff(current_date(), create_date, month) between 2 and 11 
        then concat(cast(date_diff(current_date(), create_date, month) as string), ' months')
      when date_diff(current_date(), create_date, month) between 12 and 17 then '1 year'
      when date_diff(current_date(), create_date, month) between 18 and 23 then '1.5 years'
      when date_diff(current_date(), create_date, month) between 24 and 29 then '2 years'
      else concat(cast(round(date_diff(current_date(), create_date, month) / 12.0 * 2) / 2.0 as string), ' years')
  end as tenure_label,
from
  etsy-data-warehouse-prod.rollups.seller_basics
)
, listing_views as (
select
  platform,
  seller_user_id,
  listing_id,
  sum(purchased_after_view) as purchases,
  count(distinct visit_id) as visits,
  count(sequence_number) as views,
from 
  etsy-data-warehouse-prod.analytics.listing_views 
where 1=1
  and _date between date('2025-07-01') and date('2025-07-08') 
  and platform in ('desktop','mobile_web')
group by all 
)
select
  platform,
  -- variant_id,
  -- tenure_label,
  -- months_on_etsy,
  -- count(distinct bucketing_id) as browsers,
  count(distinct lv.seller_user_id) as shops,
  count(distinct lv.listing_id) as listings,
  sum(purchases) as purchases,
  sum(visits) as visits,
  sum(views) as views,
  sum(gms_net) as total_gms,
  sum(transactions) as total_transactions,
  sum(gms_net)/sum(transactions) as aov,
from 
  listing_views lv
left join 
  tenure te  
    on lv.seller_user_id=te.user_id
left join   
  trans tr 
    on lv.seller_user_id=tr.seller_user_id
    and lv.listing_id=tr.listing_id -- only want to look at listings viewed, not all shop's gms 
group by all 
