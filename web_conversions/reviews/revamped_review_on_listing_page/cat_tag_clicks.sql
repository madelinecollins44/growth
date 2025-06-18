/* begin
create or replace temp table tag_info as (
select
	date(_partitiontime) as _date,
	v.visit_id,
	v.sequence_number,
	beacon.event_name as event_name,
  (select value from unnest(beacon.properties.key_value) where key = "tag_name") as tag_name, 
  (select value from unnest(beacon.properties.key_value) where key = "tag_type") as tag_type, 
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` vb
inner join 
  etsy-data-warehouse-prod.weblog.visits v -- only looking at browsers in the experiment 
    on v.visit_id = vb.visit_id -- everything that happens on bucketing moment and after (cant do sequence number bc there is only one)
where 1=1
  and v._date between date('2025-06-10') and date('2025-06-24') -- two weeks after last reviews experiment was ramped 
  and date(_partitiontime) between date('2025-06-10') and date('2025-06-24') -- two weeks after last reviews experiment was ramped 
  and platform in ('desktop')
  and beacon.event_name in 
    ('view_listing', 
    'listing_page_reviews_container_top_seen', -- scrolls far enough to see tags 
    'reviews_categorical_tags_seen', -- sees the tags 
    'reviews_categorical_tag_clicked', -- clicks on a tag
    'reviews_categorical_tag_filter_applied' -- clicks on a tag and reviews filter 
    )
  group by all 
);
end
*/

-- clicks by tag name
select
  tag_name,
  tag_type,
  count(sequence_number) as clicks,
  count(distinct listing_id) as listings_w_clicks
from 
  tag_info
where 
  event_type in ('reviews_categorical_tag_clicked')


-- clicks by tag name / listing attribute 
with listing_attributes as ( -- attributes from listings viewed on desktop 
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
  case when r.listing_id is not null then 1 else 0 end as has_review,
  v.listing_id,
from 
  etsy-data-warehouse-prod.analytics.listing_views v
left join 
  etsy-data-warehouse-prod.listing_mart.listings l using (listing_id)
left join
  etsy-data-warehouse-prod.listing_mart.listing_attributes a
    on a.listing_id=l.listing_id
left join 
  (select listing_id from etsy-data-warehouse-prod.listing_mart.listing_variations where variation_count > 0) va 
    on va.listing_id=l.listing_id
left join 
  (select listing_id, count(distinct transaction_id) from etsy-data-warehouse-prod.rollups.transaction_reviews where has_review > 0) r
        on r.listing_id=l.listing_id
where 1=1
  and v._date between date('2025-06-10') and date('2025-06-24') -- two weeks after last reviews experiment was ramped 
  and v.platform in ('desktop')
group by all 
)
select 
  tag_name,
  tag_type,
  count(sequence_number) as clicks,
  count(distinct listing_id) as listings_w_clicks,
from 
  listing_attributes a
left join 
  (select * from tag_info where event_type in ('reviews_categorical_tag_clicked')) c   
    using (listing_id)


-- conversion among visits that clicked on a tag
begin
create or replace temp table tag_info as (
select
	date(_partitiontime) as _date,
	v.visit_id,
	v.sequence_number,
	beacon.event_name as event_name,
  (select value from unnest(beacon.properties.key_value) where key = "tag_name") as tag_name, 
  (select value from unnest(beacon.properties.key_value) where key = "tag_type") as tag_type, 
  coalesce((select value from unnest(beacon.properties.key_value) where key = "listing_id"), regexp_extract(beacon.loc, r'listing/(\d+)')) as listing_id 
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` vb
inner join 
  etsy-data-warehouse-prod.weblog.visits v -- only looking at browsers in the experiment 
    on v.visit_id = vb.visit_id -- everything that happens on bucketing moment and after (cant do sequence number bc there is only one)
where 1=1
  and v._date between date('2025-06-10') and date('2025-06-24') -- two weeks after last reviews experiment was ramped 
  and date(_partitiontime) between date('2025-06-10') and date('2025-06-24') -- two weeks after last reviews experiment was ramped 
  and platform in ('desktop')
  and beacon.event_name in 
    ('view_listing', 
    'listing_page_reviews_container_top_seen', -- scrolls far enough to see tags 
    'reviews_categorical_tags_seen', -- sees the tags 
    'reviews_categorical_tag_clicked', -- clicks on a tag
    'reviews_categorical_tag_filter_applied' -- clicks on a tag and reviews filter 
    )
  group by all 
);
end

-- clicks by tag name / listing attribute 
with lv_stats (
select
  listing_id,
  visit_id,
  count(sequence_number) as views,
  sum(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.analytics.listing_views 
where 1=1
  and _date between date('2025-06-10') and date('2025-06-24') -- two weeks after last reviews experiment was ramped 
  and .platform in ('desktop')
group by all 
)
, lv_engagement as (
select
  v.visit_id,
  v.listing_id,
  count(v.sequence_number) as views,
  count(case when c.visit_id is not null then c.sequence_number) as cat_tag_clicks,
from
    tag_info v
left join 
    tag_info c
    on v.visit_id = c.visit_id
    and v.listing_id = c.listing_id
    and c.event_name = 'cat tag clicked'
where
    v.event_name = 'view_listing'
)
