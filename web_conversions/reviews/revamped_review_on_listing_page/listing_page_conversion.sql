with agg as (
select
  platform,
	v.visit_id,
  converted,
  event_type, 
  count(sequence_number) as views
from
	etsy-data-warehouse-prod.weblog.events e
inner join 
  etsy-data-warehouse-prod.weblog.visits v -- only looking at browsers in the experiment 
    on v.visit_id = e.visit_id -- everything that happens on bucketing moment and after (cant do sequence number bc there is only one)
where 1=1
  and v._date between date('2025-06-10') and date('2025-06-24') -- two weeks after last reviews experiment was ramped 
  and platform in ('desktop','mobile_web')
  and event_type in 
    ('view_listing', 
    'listing_page_reviews_container_top_seen', -- scrolls far enough to see tags 
    'reviews_categorical_tag_clicked', -- clicks on a tag
    'listing_page_review_engagement_frontend' -- review engagement event 
    )
  group by all 
)
select
-- view listing  
  count(distinct visit_id) as visits,
  count(distinct case when converted > 0 then visit_id end) as converted_visits,
-- view listing  
  count(distinct case when event_type in ('view_listing') then visit_id end) as lv_visits,
  count(distinct case when converted > 0 and event_type in ('view_listing') then visit_id end) as lv_converted_visits,
-- listing_page_reviews_container_top_seen 
  count(distinct case when event_type in ('listing_page_reviews_container_top_seen') then visit_id end) as reviews_seen_visits,
  count(distinct case when converted > 0 and event_type in ('listing_page_reviews_container_top_seen') then visit_id end) as reviews_seen_converted_visits,
  -- reviews_categorical_tag_clicked
  count(distinct case when event_type in ('reviews_categorical_tag_clicked') then visit_id end) as cattag_click_visits,
  count(distinct case when converted > 0 and event_type in ('reviews_categorical_tag_clicked') then visit_id end) as cattag_click_converted_visits,
  -- listing_page_review_engagement_frontend
  count(distinct case when event_type in ('listing_page_review_engagement_frontend') then visit_id end) as review_engagement_visits,
  count(distinct case when converted > 0 and event_type in ('listing_page_review_engagement_frontend') then visit_id end) as review_engagement_converted_visits,
from 
  agg
