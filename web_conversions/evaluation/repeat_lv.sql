------------------------------------------------------------------------------------
-- HOW MANY TIMES DOES A BROWSER VIEW THE SAME LISTING?
------------------------------------------------------------------------------------
------ get all listing views from each view
with all_lv as (
select
  split(visit_id,'.')[safe_offset(0)] as browser_id,
  listing_id,
  count(sequence_number) as listing_views,
  sum(purchased_after_view) as purchases
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all 
)
select
  count(distinct listing_id) as listings_viewed,
  -- listing view counts
  sum(listing_views) as total_lv,
  sum(case when listing_views = 1 then listing_views end) as one_time_lv,
  sum(case when listing_views > 1 then listing_views end) as lv_1plus,
  sum(case when listing_views > 2 then listing_views end) as lv_2plus,
  sum(case when listing_views > 3 then listing_views end) as lv_3plus,
  sum(case when listing_views > 4 then listing_views end) as lv_4plus,
  sum(case when listing_views > 5 then listing_views end) as lv_5plus,
  --browser counts
  count(distinct browser_id) as browsers,
  count(distinct case when listing_views = 1 then browser_id end) as browsers_w_1_lv,
  count(distinct case when listing_views > 1 then browser_id end) as browsers_w_1plus_lv,
  count(distinct case when listing_views > 2 then browser_id end) as browsers_w_2plus_lv,
  count(distinct case when listing_views > 3 then browser_id end) as browsers_w_3plus_lv,
  count(distinct case when listing_views > 4 then browser_id end) as browsers_w_4plus_lv,
  count(distinct case when listing_views > 5 then browser_id end) as browsers_w_5plus_lv,
  -- browsers w/ purchase
  count(distinct case when purchases > 0 then browser_id end) as purchase_browsers,
  count(distinct case when listing_views = 1 and purchases > 0 then browser_id end) as purchase_browsers_w_1_lv,
  count(distinct case when listing_views > 1 and purchases > 0 then browser_id end) as purchase_browsers_w_1plus_lv,
  count(distinct case when listing_views > 2 and purchases > 0 then browser_id end) as purchase_browsers_w_2plus_lv,
  count(distinct case when listing_views > 3 and purchases > 0 then browser_id end) as purchase_browsers_w_3plus_lv,
  count(distinct case when listing_views > 4 and purchases > 0 then browser_id end) as purchase_browsers_w_4plus_lv,
  count(distinct case when listing_views > 5 and purchases > 0 then browser_id end) as purchase_browsers_w_5plus_lv,
from all_lv

------ browser level stats
with all_lv as (
select
  split(visit_id,'.')[safe_offset(0)] as browser_id,
  listing_id,
  count(sequence_number) as listing_views,
  sum(purchased_after_view) as purchases
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all 
)
, browser_stats as (
select
  browser_id,
  count(distinct listing_id) as listings_seen
from all_lv
group by all 
)
select
  count(distinct browser_id) as browsers,
  approx_quantiles(listings_seen, 4)[OFFSET(1)] AS q1,
  approx_quantiles(listings_seen, 4)[OFFSET(2)] AS median,
  approx_quantiles(listings_seen, 4)[OFFSET(3)] AS q3,
  approx_quantiles(listings_seen, 4)[OFFSET(4)] AS q4,
  avg(listings_seen) as avg_listings_seen
from 
  browser_stats
group by all 

  
------ browser, listing stats
  with all_lv as (
select
  split(visit_id,'.')[safe_offset(0)] as browser_id,
  listing_id,
  count(sequence_number) as listing_views,
  sum(purchased_after_view) as purchases
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all 
)
, browser_stats as (
select
  browser_id,
  listing_id,
  listing_views
from all_lv
group by all 
)
select
  count(distinct browser_id) as browsers,
  approx_quantiles(listing_views, 4)[OFFSET(1)] AS q1,
  approx_quantiles(listing_views, 4)[OFFSET(2)] AS median,
  approx_quantiles(listing_views, 4)[OFFSET(3)] AS q3,
  approx_quantiles(listing_views, 4)[OFFSET(4)] AS q4,
  avg(listing_views) as avg_views_per_listing
from 
  browser_stats
group by all 
------------------------------------------------------------------------------------
-- HOW MANY TIMES DOES A BROWSER VIEW THE LISTINGS IN THE SAME TAXONOMY?
------------------------------------------------------------------------------------
-- get all listing views from each view
with all_lv as (
select
  split(visit_id,'.')[safe_offset(0)] as browser_id,
  top_category,
  count(sequence_number) as listing_views,
  sum(purchased_after_view) as purchases
from 
  etsy-data-warehouse-prod.analytics.listing_views
inner join 
  etsy-data-warehouse-prod.rollups.active_listing_basics
    using (listing_id)
where 
  _date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all 
)
select
  top_category,
  -- count(distinct listing_id) as listings_viewed,
  -- listing view counts
  sum(listing_views) as total_lv,
  sum(case when listing_views = 1 then listing_views end) as one_time_lv,
  sum(case when listing_views > 1 then listing_views end) as lv_1plus,
  sum(case when listing_views > 2 then listing_views end) as lv_2plus,
  sum(case when listing_views > 3 then listing_views end) as lv_3plus,
  sum(case when listing_views > 4 then listing_views end) as lv_4plus,
  sum(case when listing_views > 5 then listing_views end) as lv_5plus,
  --browser counts
  count(distinct browser_id) as browsers,
  count(distinct case when listing_views = 1 then browser_id end) as browsers_w_1_lv,
  count(distinct case when listing_views > 1 then browser_id end) as browsers_w_1plus_lv,
  count(distinct case when listing_views > 2 then browser_id end) as browsers_w_2plus_lv,
  count(distinct case when listing_views > 3 then browser_id end) as browsers_w_3plus_lv,
  count(distinct case when listing_views > 4 then browser_id end) as browsers_w_4plus_lv,
  count(distinct case when listing_views > 5 then browser_id end) as browsers_w_5plus_lv,
  -- browsers w/ purchase
  count(distinct case when purchases > 0 then browser_id end) as purchase_browsers,
  count(distinct case when listing_views = 1 and purchases > 0 then browser_id end) as purchase_browsers_w_1_lv,
  count(distinct case when listing_views > 1 and purchases > 0 then browser_id end) as purchase_browsers_w_1plus_lv,
  count(distinct case when listing_views > 2 and purchases > 0 then browser_id end) as purchase_browsers_w_2plus_lv,
  count(distinct case when listing_views > 3 and purchases > 0 then browser_id end) as purchase_browsers_w_3plus_lv,
  count(distinct case when listing_views > 4 and purchases > 0 then browser_id end) as purchase_browsers_w_4plus_lv,
  count(distinct case when listing_views > 5 and purchases > 0 then browser_id end) as purchase_browsers_w_5plus_lv,
from all_lv
group by all 
order by 2 desc 
------------------------------------------------------------------------------------
-- HOW MANY TIMES DOES A BROWSER VIEW THE LISTINGS IN THE SAME PRICE BUCKET?
------------------------------------------------------------------------------------
-- -- get all listing views from each view
with all_lv as (
select
  split(visit_id,'.')[safe_offset(0)] as browser_id,
  case 
    when coalesce(b.price_usd, v.price_usd) > 0 and coalesce(b.price_usd, v.price_usd) <= 10 then'$0-$10'
    when coalesce(b.price_usd, v.price_usd) > 10 and coalesce(b.price_usd, v.price_usd) <= 20 then'$10-$20'
    when coalesce(b.price_usd, v.price_usd) > 20 and coalesce(b.price_usd, v.price_usd) <= 40 then'$20-$40'
    when coalesce(b.price_usd, v.price_usd) > 40 and coalesce(b.price_usd, v.price_usd) <= 60 then'$40-$60'
    when coalesce(b.price_usd, v.price_usd) > 60 and coalesce(b.price_usd, v.price_usd) <= 80 then'$60-$80'
    when coalesce(b.price_usd, v.price_usd) > 80 and coalesce(b.price_usd, v.price_usd) <= 100 then'$80-$100'
    when coalesce(b.price_usd, v.price_usd) > 100 and coalesce(b.price_usd, v.price_usd) <= 120 then'$100-$120'
    when coalesce(b.price_usd, v.price_usd) > 120 and coalesce(b.price_usd, v.price_usd) <= 150 then'$120-$150'
    else 'over $150' 
  end as item_price_bucket,
  listing_id,
  count(sequence_number) as listing_views,
  sum(purchased_after_view) as purchases
from 
  etsy-data-warehouse-prod.analytics.listing_views v
inner join 
  etsy-data-warehouse-prod.rollups.active_listing_basics b
    using (listing_id)
where 
  _date >= current_date-30
  and platform in ('mobile_web','desktop')
group by all 
)
select
  item_price_bucket,
  -- count(distinct listing_id) as listings_viewed,
  -- listing view counts
  sum(listing_views) as total_lv,
  sum(case when listing_views = 1 then listing_views end) as one_time_lv,
  sum(case when listing_views > 1 then listing_views end) as lv_1plus,
  sum(case when listing_views > 2 then listing_views end) as lv_2plus,
  sum(case when listing_views > 3 then listing_views end) as lv_3plus,
  sum(case when listing_views > 4 then listing_views end) as lv_4plus,
  sum(case when listing_views > 5 then listing_views end) as lv_5plus,
  --browser counts
  count(distinct browser_id) as browsers,
  count(distinct case when listing_views = 1 then browser_id end) as browsers_w_1_lv,
  count(distinct case when listing_views > 1 then browser_id end) as browsers_w_1plus_lv,
  count(distinct case when listing_views > 2 then browser_id end) as browsers_w_2plus_lv,
  count(distinct case when listing_views > 3 then browser_id end) as browsers_w_3plus_lv,
  count(distinct case when listing_views > 4 then browser_id end) as browsers_w_4plus_lv,
  count(distinct case when listing_views > 5 then browser_id end) as browsers_w_5plus_lv,
  -- browsers w/ purchase
  count(distinct case when purchases > 0 then browser_id end) as purchase_browsers,
  count(distinct case when listing_views = 1 and purchases > 0 then browser_id end) as purchase_browsers_w_1_lv,
  count(distinct case when listing_views > 1 and purchases > 0 then browser_id end) as purchase_browsers_w_1plus_lv,
  count(distinct case when listing_views > 2 and purchases > 0 then browser_id end) as purchase_browsers_w_2plus_lv,
  count(distinct case when listing_views > 3 and purchases > 0 then browser_id end) as purchase_browsers_w_3plus_lv,
  count(distinct case when listing_views > 4 and purchases > 0 then browser_id end) as purchase_browsers_w_4plus_lv,
  count(distinct case when listing_views > 5 and purchases > 0 then browser_id end) as purchase_browsers_w_5plus_lv,
from all_lv
group by all 
order by 1 asc 

--------------------------------------------------------------------------------------------------
--TESTING
--------------------------------------------------------------------------------------------------
-- TEST 1: make sure no browsers are missing from table
select
 sum(case when v.browser_id is null then 1 else 0 end) as missing_browser_id
from 
  etsy-data-warehouse-prod.analytics.listing_views lv
left join 
  etsy-data-warehouse-prod.weblog.visits v 
    on v.browser_id = split(lv.visit_id,'.')[safe_offset(0)] 
where 
  v._date >= current_date-30
  and lv._date >= current_date-30
  and lv.platform in ('mobile_web','desktop')
group by all
-- no missing browser_ids
