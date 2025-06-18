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
