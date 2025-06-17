with listing_engagements as (
select
	date(_partitiontime) as _date,
	v.visit_id,
	v.sequence_number,
	beacon.event_name as event_name,
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` vb
inner join 
  etsy-data-warehouse-prod.weblog.visits v -- only looking at browsers in the experiment 
    on v.visit_id = vb.visit_id -- everything that happens on bucketing moment and after (cant do sequence number bc there is only one)
where 1=1
	and date(_partitiontime) between date('2025-03-01') and date('2025-03-15') -- two weeks before first reviews experiment was ramped 
  -- and date(_partitiontime) between date('2025-06-10') and date('2025-06-24') -- two weeks after last reviews experiment was ramped 
  and beacon.event_name  in ('view_listing','listing_page_reviews_seen','listing_page_reviews_container_top_seen','listing_page_review_engagement_frontend','listing_page_reviews_pagination','appreciation_photo_overlay_opened','sort_reviews','reviews_categorical_tag_clicked','listing_page_reviews_content_toggle_opened')
  group by all 
)
, listing_attributes as (
select
  is_digital,
  top_category,
  is_personalizable, 
  case when va.listing_id is not null then 1 else 0 end as has_variation,
  case 
    when (l.price_usd/100) > 100 then 'high' 
    when (l.price_usd/100) > 30 then 'mid' 
    when (l.price_usd/100) <= 30 then 'low' 
  end as listing_price, -- uses same logic as segment
  listing_id
from 
  etsy-data-warehouse-prod.listing_mart.listings l
left join
  etsy-data-warehouse-prod.listing_mart.listing_attributes a 
    on a.listing_id=l.listing_id
left join 
  (select listing_id from etsy-data-warehouse-prod.listing_mart.listing_variations where variation_count > 0) va -- if a listing has variation 
    on va.listing_id=l.listing_id
group by all 
)














, lv_stats as (
select
  lv.platform,
	lv.listing_id,
	case
  	when coalesce((p.price_usd/100), a.price_usd) > 100 then 'high stakes'
  	else 'low stakes'
  end as listing_type,
	case when a.listing_id is null then 1 else 0 end as missing_in_analytics,
	count(lv.visit_id) as listing_views,
	count(a.visit_id) as a_listing_views,
	count(case when saw_reviews = 1 then lv.visit_id end) as views_and_reviews_seen,
	sum(case when saw_reviews = 1 then purchased_after_view end) as saw_reviews_and_purchased,
	sum(purchased_after_view) as purchases
from 
	listing_views lv
left join 
	etsy-data-warehouse-prod.analytics.listing_views a
		on lv.listing_id=cast(a.listing_id as string)
		and lv.visit_id=a.visit_id
		and lv.sequence_number=a.sequence_number	
left join 
  etsy-data-warehouse-prod.listing_mart.listings p 
    on cast(p.listing_id as string)=lv.listing_id
where a._date >=current_date-30
group by all
)
