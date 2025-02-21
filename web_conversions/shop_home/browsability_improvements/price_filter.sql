------------------------------------------------------------------------------------------
-- LISTING VIEWED + ACTIVE LISTINGS BY PRICE 
------------------------------------------------------------------------------------------
select
	case
  	when coalesce((a.price_usd/100), lv.price_usd) < 1 then 'Less than $1'
    when coalesce((a.price_usd/100), lv.price_usd) >= 1 and coalesce((a.price_usd/100), lv.price_usd) < 5 then '$1-$4.99'
    when coalesce((a.price_usd/100), lv.price_usd) >= 5 and coalesce((a.price_usd/100), lv.price_usd) < 10 then '$5-$9.99'
    when coalesce((a.price_usd/100), lv.price_usd) >= 10 and coalesce((a.price_usd/100), lv.price_usd) < 25 then '$10-$24.99'
    when coalesce((a.price_usd/100), lv.price_usd) >= 25 and coalesce((a.price_usd/100), lv.price_usd) < 50 then '$25-$49.99'
    when coalesce((a.price_usd/100), lv.price_usd) >= 50 and coalesce((a.price_usd/100), lv.price_usd) < 75 then '$50-$74.99'
    when coalesce((a.price_usd/100), lv.price_usd) >= 75 and coalesce((a.price_usd/100), lv.price_usd) < 100 then '$75-$99.99'
    else '100+'
    -- when coalesce((a.price_usd/100), lv.price_usd) >= 100 and coalesce((a.price_usd/100), lv.price_usd) < 150 then '$100-$49.99'
    -- when coalesce((a.price_usd/100), lv.price_usd) >= 50 and coalesce((a.price_usd/100), lv.price_usd) < 75 then '$50-$4.99'
  end as listing_price,

	count(a.listing_id) as active_listings,

  case when a.listing_id is null then 1 else 0 end as missing_in_analytics,
  count(distinct lv.listing_id) as listings_viewed,
	count(lv.visit_id) as listing_views,
  sum(purchased_after_view) as purchases,
  
  count(distinct case when referring_page_event in ('shop_home') then lv.listing_id end) as shop_home_listings_viewed,
	count(case when referring_page_event in ('shop_home') then lv.visit_id end) as shop_home_listing_views,
	sum(case when referring_page_event in ('shop_home') then purchased_after_view end) as shop_home_purchases

from 
  etsy-data-warehouse-prod.rollups.active_listing_basics a
left join 
 (select * from etsy-data-warehouse-prod.analytics.listing_views where platform in ('mobile_web','desktop')) lv 
    on cast(a.listing_id as int64)=lv.listing_id
where  
  lv._date >=current_date-30
group by all
