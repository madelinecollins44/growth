------------------------------------------------------------------------------------
-- HOW MANY TIMES DOES A BROWSER VIEW THE SAME LISTING?
------------------------------------------------------------------------------------
-- get all listing views from each view
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
------------------------------------------------------------------------------------
-- HOW MANY TIMES DOES A BROWSER VIEW THE LISTINGS IN THE SAME TAXONOMY?
------------------------------------------------------------------------------------
