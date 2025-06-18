select
	date(_partitiontime) as _date,
	v.visit_id,
  v.converted,
	beacon.event_name as event_name,
  (select value from unnest(beacon.properties.key_value) where key = "tag_name") as tag_name, 
  (select value from unnest(beacon.properties.key_value) where key = "tag_type") as tag_type, 
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
    'reviews_categorical_tag_clicked', -- clicks on a tag
    'listing_page_review_engagement_frontend' -- review engagement event 
    )
  group by all 
);
