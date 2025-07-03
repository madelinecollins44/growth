select
  is_digital,
  top_category,
  is_personalizable, 
  has_variation,
  listing_price,
  has_reviews,
  count(distinct listing_id) as listings, 
  sum(listing_views) as listing_views, 
  sum(reviews_seen) as reviews_seen, 
  sum(reviews_top_container_seen) as reviews_top_container_seen, 
  sum(listing_page_review_engagements) as listing_page_review_engagements, 
  sum(paginations) as paginations, 
  sum(photo_opens) as photo_opens, 
  sum(review_sorts) as review_sorts, 
  sum(cat_tag_clicks) as cat_tag_clicks, 
  sum(cat_tags_seen) as cat_tags_seen, 
  sum(toggle_opens) as toggle_opens, 
from
    etsy-data-warehouse-dev.madelinecollins.segments_and_events 
group by all 
