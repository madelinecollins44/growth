/* SIGNED IN VS SIGNED OUT CONVERSION */
with shops_wo_reviews as (
select 
  shop_id,
  shop_name,
  seller_user_id,
  count(distinct transaction_id) as transactions,
  sum(has_review) as total_reviews
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
inner join  
  etsy-data-warehouse-prod.rollups.seller_basics using (shop_id)
group by all 
)
select
  case when total_reviews = 0 then 0 else 1 end as has_shop_reviews,
  case when transactions = 0 then 0 else 1 end as has_transactions,
  case when user_id is null or user_id= 0 then 0 else 1 end as signed_in,
  -- seller_user_id,
  sum(purchased_after_view) as purchases,
  count(sequence_number) as views, 
from 
  etsy-data-warehouse-prod.analytics.listing_views l
left join 
  shops_wo_reviews r using (seller_user_id)
inner join 
  (select
      user_id,
      visit_id
    from 
      etsy-data-warehouse-prod.weblog.visits
    where _date >= current_date-30) v
      on l.visit_id=v.visit_id
where _date >= current_date-30 
group by all 
