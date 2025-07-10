

create or replace table `etsy-data-warehouse-dev.madelinecollins.tag_info` as (
select
  variant_id,
	date(_partitiontime) as _date,
	v.visit_id,
  v.bucketing_id,
	vb.sequence_number,
	beacon.event_name as event_name,
  (select value from unnest(beacon.properties.key_value) where key = "tag_name") as tag_name, 
  (select value from unnest(beacon.properties.key_value) where key = "tag_type") as tag_type, 
  regexp_extract(beacon.loc, r'listing/(\d+)') as listing_id 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` vb
inner join 
  etsy-data-warehouse-dev.madelinecollins.ab_first_bucket  v -- only looking at browsers in the experiment 
    on split(vb.visit_id, ".")[0] = v.bucketing_id 
    and v.bucketing_ts <= timestamp_millis(vb.beacon.timestamp) 
where 1=1
  and date(_partitiontime) >= date('2025-06-10') -- looking after experiment was ramped 
  and beacon.event_name in 
    ('reviews_categorical_tags_seen', -- sees the tags 
    'reviews_categorical_tag_clicked', -- clicks on a tag
    'reviews_categorical_tag_filter_applied' -- clicks on a tag and reviews filter 
    )
  group by all 
);

-- funnel view
select
  variant_id,
  event_name,
  count(sequence_number) as clicks,
  count(distinct listing_id) as listings_w_clicks
from 
  etsy-data-warehouse-dev.madelinecollins.tag_info 
group by all 
order by 1,2 desc

	
-- clicks by tag name
select
  variant_id,
  tag_name,
  tag_type,
  count(sequence_number) as clicks,
  count(distinct listing_id) as listings_w_clicks
from 
  etsy-data-warehouse-dev.madelinecollins.tag_info 
where 
  event_name in ('reviews_categorical_tag_clicked')
  and variant_id in ('on')
group by all 
order by 1,2,3 desc 

-- clicks by tag name / listing attribute 
with listing_attributes as (
select
  coalesce(is_digital,0) as is_digital, 
  coalesce(top_category,'n/a') as top_category,
  coalesce(is_personalizable,0) as is_personalizable,
  coalesce((case when va.listing_id is not null then 1 else 0 end),0) as has_variation,
  coalesce((case 
    when (l.price_usd/100) > 100 then 'high' 
    when (l.price_usd/100) > 30 then 'mid' 
    when (l.price_usd/100) <= 30 then 'low' 
  end),'n/a') as listing_price, -- uses same logic as segment
  coalesce(max(case when r.reviews > 0 or r.listing_id is not null then 1 else 0 end),0) as has_reviews,
  v.listing_id,
from 
  (select distinct listing_id from etsy-data-warehouse-dev.madelinecollins.tag_info) v 
left join 
  etsy-data-warehouse-prod.listing_mart.listings l 
    on cast(l.listing_id as string)=v.listing_id 
left join
  etsy-data-warehouse-prod.listing_mart.listing_attributes a 
    on a.listing_id=l.listing_id
left join 
  (select listing_id from etsy-data-warehouse-prod.listing_mart.listing_variations where variation_count > 0) va 
    on va.listing_id=l.listing_id
left join   
  (select listing_id, count(distinct transaction_id) as reviews from etsy-data-warehouse-prod.rollups.transaction_reviews where has_review > 0 group by all ) r   
    on l.listing_id=r.listing_id
group by all 
)
select
  variant_id,
  is_digital, 
  top_category,
  is_personalizable,
  has_variation,
  listing_price, -- uses same logic as segment
  has_reviews,
  tag_name,
  tag_type,
  count(sequence_number) as clicks,
  count(distinct listing_id) as listings_w_clicks
from 
  etsy-data-warehouse-dev.madelinecollins.tag_info 
left join
  listing_attributes using (listing_id)
where 
  event_name in ('reviews_categorical_tag_clicked')
  and variant_id in ('on')
group by all 
order by 1,2,3 desc 


-- conversion among visits that clicked on a tag
with lv_stats as (
select
  listing_id,
  visit_id,
  count(sequence_number) as views,
  sum(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.analytics.listing_views 
where 1=1
  and _date between date('2025-06-10') and date('2025-06-24') -- two weeks after last reviews experiment was ramped 
  and platform in ('desktop')
group by all 
)
, lv_engagement as (
select
  v.visit_id,
  v.listing_id,
  count(distinct concat(v.visit_id,v.sequence_number)) as views,
  count(distinct case when c.visit_id is not null then concat(c.visit_id,c.sequence_number) end) as cat_tag_clicks,
from
    etsy-data-warehouse-dev.madelinecollins.tag_info  v
left join 
    etsy-data-warehouse-dev.madelinecollins.tag_info  c
    on v.visit_id = c.visit_id
    and v.listing_id = c.listing_id
    and c.event_name in ('reviews_categorical_tag_clicked')
where
    v.event_name = 'view_listing'
group by all
)
select
  case when cat_tag_clicks > 0 then 1 else 0 end as clicked_on_cattag,
  sum(s.views) as listing_views,
  sum(cat_tag_clicks) as clicks,
  sum(purchases) as purchases,
from 
  lv_stats s
left join 
  lv_engagement e
    on e.listing_id=cast(s.listing_id as string)
    and s.visit_id=e.visit_id
group by all
