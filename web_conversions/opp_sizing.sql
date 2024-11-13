------------------------------------------------------------
--BUYER SEGMENT
------------------------------------------------------------
-- begin
-- create or replace temp table buyer_segments as (select * from etsy-data-warehouse-prod.rollups.buyer_segmentation_vw where as_of_date >= current_date-60);
-- end 

select
  buyer_segment,
  count(distinct visit_id) as traffic,
  count(distinct case when converted > 0 then visit_id end) as converted_visits,
  sum(total_gms) as total_gms
from 
  etsy-data-warehouse-prod.weblog.visits v
left join 
  etsy-data-warehouse-prod.user_mart.user_mapping um  
    on v.user_id=um.user_id
left join 
   etsy-bigquery-adhoc-prod._script1dbdadf400b7c4c2ad8ee807a3e3ac5028d9345c.buyer_segments bs
    on um.mapped_user_id=bs.mapped_user_id
where _date >= current_date-30
-- and v.platform in ('mobile_web','desktop')
group by all 

  --signed in vs signed out 
select
  case when user_id is not null or user_id != 0 then 'signed in' else 'signed out' end as user_status,
  count(distinct visit_id) as traffic,
  count(distinct case when converted > 0 then visit_id end) as converted_visits,
  sum(total_gms) as total_gms
from 
  etsy-data-warehouse-prod.weblog.visits v
where _date >= current_date-30
and v.platform in ('mobile_web','desktop')
group by all 
------------------------------------------------------------
--REPORTING CHANNEL
------------------------------------------------------------
select
case 
      when top_channel in ('direct') then 'Direct'
      when top_channel in ('dark') then 'Dark'
      when top_channel in ('internal') then 'Internal'
      when top_channel in ('seo') then 'SEO'
      when top_channel like 'social_%' then 'Non-Paid Social'
      when top_channel like 'email%' then 'Email'
      when top_channel like 'push_%' then 'Push'
      when top_channel in ('us_paid','intl_paid') then
        case
          when (second_channel like '%gpla' or second_channel like '%bing_plas') then 'PLA'
          when (second_channel like '%_ppc' or second_channel like 'admarketplace') then case
          when third_channel like '%_brand' then 'SEM - Brand' else 'SEM - Non-Brand'
          end
      when second_channel='affiliates' then 'Affiliates'
      when (second_channel like 'facebook_disp%' or second_channel like 'pinterest_disp%') then 'Paid Social'
      when second_channel like '%native_display' then 'Display'
      when second_channel in ('us_video','intl_video') then 'Video' else 'Other Paid' end
      else 'Other Non-Paid' 
      end as reporting_channel,
  count(distinct visit_id) as traffic,
  count(distinct case when converted > 0 then visit_id end) as converted_visits,
  sum(total_gms) as total_gms
from 
  etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30
and v.platform in ('mobile_web','desktop')
group by all 

------------------------------------------------------------
--LISTING CATEGORY
------------------------------------------------------------
with listing_views as (
select
  listing_id,
  count(listing_id) as views,
  sum(purchased_after_view) as purchases,
  avg(price_usd) as average_listing_price_usd,
  avg(shipping_price_usd) as average_shipping_price_usd
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all 
)
, active_listings as (
SELECT 
  listing_id, 
  a.is_active, 
  taxonomy_id,  
SPLIT(t.full_path, '.')[safe_offset(0)] AS top_level_taxonomy,
SPLIT(t.full_path, '.')[safe_offset(1)] AS l2_taxonomy--, -- subcategory  
FROM 
  `etsy-data-warehouse-prod.listing_mart.listing_attributes`  a
JOIN 
  `etsy-data-warehouse-prod.structured_data.taxonomy` t 
    USING (taxonomy_id)
WHERE 
  a.is_active = 1 
)
select
  a.top_level_taxonomy,
  count(distinct a.listing_id) as active_listings,
  count(distinct v.listing_id) as listings_viewed,
  sum(v.views) as listing_views,
  sum(v.purchases) as purchases,
  sum(v.purchases) / sum(v.views) as purchase_rate,
  --price 
  avg(average_listing_price_usd) as average_listing_price_usd,
  max(average_listing_price_usd) as max_listing_price_usd,
  min(average_listing_price_usd) as min_listing_price_usd,
    --shipping costs
  avg(average_shipping_price_usd) as average_shipping_price_usd,
  max(average_shipping_price_usd) as max_shipping_price_usd,
  min(average_shipping_price_usd) as min_shipping_price_usd
from  
  active_listings a
left join 
  listing_views v using (listing_id)
group by all 

--including listing gms
  with listing_views as (
select
  listing_id,
  count(listing_id) as views,
  sum(purchased_after_view) as purchases,
  avg(price_usd) as average_listing_price_usd,
  avg(shipping_price_usd) as average_shipping_price_usd
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  -- and platform in ('mobile_web','desktop')
group by all 
)
, active_listings as (
SELECT 
  listing_id, 
  a.is_active, 
  taxonomy_id,  
SPLIT(t.full_path, '.')[safe_offset(0)] AS top_level_taxonomy,
SPLIT(t.full_path, '.')[safe_offset(1)] AS l2_taxonomy--, -- subcategory  
FROM 
  `etsy-data-warehouse-prod.listing_mart.listing_attributes`  a
JOIN 
  `etsy-data-warehouse-prod.structured_data.taxonomy` t 
    USING (taxonomy_id)
WHERE 
  a.is_active = 1 
)
, listing_orders as (
select
  listing_id,
  sum(total_orders) as total_orders,
  sum(total_gms) as total_gms,
  sum(total_quantity_sold) as total_quantity_sold
from 
  etsy-data-warehouse-prod.listing_mart.listing_gms
where 
  is_active=1 
group by all 
)
select
  top_level_taxonomy,
  count(distinct a.listing_id) as active_listings,
  count(distinct v.listing_id) as viewed_listings,
  avg(v.average_listing_price_usd) as average_listing_price_usd, 
  sum(v.views) as listing_views,
  sum(v.purchases) as purchases,
  sum(o.total_orders) as total_orders,
  sum(o.total_gms) as total_gms,
  sum(o.total_quantity_sold) as total_quantity_sold,
from
  active_listings a
left join 
  listing_views v using (listing_id) --only looks at listings viewed in the last 30 days, but purchase metrics forever
left join 
  listing_orders o
    on v.listing_id=o.listing_id
group by all 
  
  --including transaction
  with listing_views as (
select
  listing_id,
  count(listing_id) as views,
  sum(purchased_after_view) as purchases,
  avg(price_usd) as average_listing_price_usd
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all 
)
-- , listing_transactions as (
-- select
--   listing_id,
--   count(transaction_id) as transactions,
--   avg(usd_price) as avg_trans_price_usd,
--   sum(quantity) as total_listings_bought
-- from  
--   etsy-data-warehouse-prod.transaction_mart.all_transactions
-- where 
--   date >= current_date-30
-- group by all 
-- )
, active_listings as (
SELECT 
  listing_id, 
  a.is_active, 
  taxonomy_id,  
SPLIT(t.full_path, '.')[safe_offset(0)] AS top_level_taxonomy,
SPLIT(t.full_path, '.')[safe_offset(1)] AS l2_taxonomy--, -- subcategory  
FROM 
  `etsy-data-warehouse-prod.listing_mart.listing_attributes`  a
JOIN 
  `etsy-data-warehouse-prod.structured_data.taxonomy` t 
    USING (taxonomy_id)
WHERE a.is_active = 1 
)
select
  a.top_level_taxonomy,
  count(distinct a.listing_id) as active_listings,
  count(distinct v.listing_id) as listings_viewed,
  sum(v.views) as listing_views,
  sum(t.transactions) as transactions,
  avg(average_price_usd) as avg_average_price_usd,
  sum(v.purchases) / sum(v.views) as conversion_rate_2,
  sum(t.transactions) / sum(v.views) as conversion_rate,
  sum(t.total_listings_bought) as total_listings_bought,
  avg(t.avg_trans_price_usd) as avg_trans_price_usd
from  
  active_listings a
left join 
  listing_views v using (listing_id)
left join listing_transactions t 
  on a.listing_id=t.listing_id
group by all 
------------------------------------------------------------
--PLATFORM
------------------------------------------------------------
select
  browser_platform,
  platform,
  count(distinct visit_id) as traffic,
  count(distinct case when converted > 0 then visit_id end) as converted_visits,
  sum(total_gms) as total_gms
from 
  etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30
group by all 

------------------------------------------------------------
--ENTRY VS EXIT POINTS
------------------------------------------------------------
--landing events
select
  landing_event,
  count(distinct visit_id) as traffic,
  count(distinct case when converted > 0 then visit_id end) as converted_visits,
  sum(total_gms) as total_gms
from 
  etsy-data-warehouse-prod.weblog.visits v
where _date >= current_date-30
and v.platform in ('mobile_web','desktop')
group by all 

--understanding traffic of site
with primary_events as (
select
  event_type,
  visit_id,
  count(visit_id) as pageviews
from 
  etsy-data-warehouse-prod.weblog.events
where 
  page_view =1 
  and _date >= current_date-30
group by all 
)
select
  e.event_type,
  count(distinct e.visit_id) as total_visits,
  sum(pageviews) as pageviews,
  sum(total_gms) as total_gms
from   
  primary_events e
left join 
  etsy-data-warehouse-prod.weblog.visits v
    using (visit_id)
where
  v._date >= current_date-30
  and v.platform in ('mobile_web','desktop')
group by all 
  
------------------------------------------------------------
--PAST YEAR GMS
---pulls past year gms from most recent visit in last 30 days
------------------------------------------------------------
-- create or replace table etsy-data-warehouse-dev.madelinecollins.past_year_gms_visits as (
-- with all_visits as (
-- select
--   platform,
--   mapped_user_id,
--   max(_date) as _date, --> most recent visit date
--   count(distinct visit_id) as total_visits,
--   sum(total_gms) as total_gms,
--   count(distinct case when converted > 0 then visit_id end) as converted_visits, 
-- from 
--   etsy-data-warehouse-prod.weblog.visits v
-- left join 
--   etsy-data-warehouse-prod.user_mart.user_mapping um  
--     on v.user_id=um.user_id
-- where _date >= current_date-30
-- group by all
-- )
-- -- , past_year_gms as (
-- select
--   platform,
--   a.mapped_user_id,
--   total_visits,
--   total_gms,
--   converted_visits, 
--   cast(coalesce(sum(CASE WHEN date between date_sub(_date, interval 365 DAY) and _date THEN gms_net END), 0) as int64) AS past_year_gms
-- from    
--   all_visits a
-- left join 
--   `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` e
--     on a.mapped_user_id = e.mapped_user_id 
--     and e.date <= a._date-1 
--     and market <> 'ipp'
-- group by all
-- );

select
  platform,
  case
    when past_year_gms <=0 then '1. 0'
    when past_year_gms > 0 and past_year_gms <= 50 then '2. 0-50'
    when past_year_gms > 50 and past_year_gms <= 100 then '3. 50-100'
    when past_year_gms > 100 and past_year_gms <= 200 then '4. 100-200'
    when past_year_gms > 200 and past_year_gms <= 300 then '5. 200-300'
    when past_year_gms > 300 and past_year_gms <= 400 then '6. 300-400'
    when past_year_gms > 400 and past_year_gms <= 500 then '7. 400-500'
    when past_year_gms > 500 and past_year_gms <= 600 then '8. 500-600'
    when past_year_gms > 600 and past_year_gms <= 700 then '9. 600-700'
    when past_year_gms > 700 and past_year_gms <= 800 then '10. 700-800'
    when past_year_gms > 800 and past_year_gms <= 900 then '11. 800-900'
    when past_year_gms > 900 and past_year_gms <= 1000 then '12. 900-1000'
    else '13. 1000+'
  end as past_year_gms,
  sum(total_visits) as total_visits,
  sum(total_gms) as gms,
  sum(converted_visits) as converted_visits,
  count(distinct mapped_user_id) as users
from etsy-data-warehouse-dev.madelinecollins.past_year_gms_visits
group by all 
--testing 
--389342374, past year gms = 0, gms = 124.42
--455809418, past year gms = 0, gms = 0
--83955870, 46 visits, convert 0, past_year_gms between 70-80 ,gms 0
--183063113, past year gms between 70-80, 27 visits, 5.15 gms, 2 coverted visits

select * from `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` where mapped_user_id = 83955870
select sum(gms_net) from `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` where mapped_user_id = 183063113 and date >= '2023-11-10'
--83955870: 71.49583862
--183063113: 74.752552

select
  platform,
  mapped_user_id,
  max(_date) as _date, --> most recent visit date
  count(distinct visit_id) as total_visits,
  sum(total_gms) as total_gms,
  count(distinct case when converted > 0 then visit_id end) as converted_visits, 
from 
  etsy-data-warehouse-prod.weblog.visits v
left join 
  etsy-data-warehouse-prod.user_mart.user_mapping um  
    on v.user_id=um.user_id
where _date >= current_date-30
and mapped_user_id = 183063113
group by all
--389342374: last visit date was 11.6 where they made a purchase, but last purchase before that was 7/1/23 which was more than a year before last visit 
--455809418: last visit date was 11.10 where they made a purchase, but last purchase before that was 3/6/23 which was more than a year before last visit 
--83955870: last visit date was 11.05
--183063113: last visit date was 11.10
