-------------------------------------------------------------------------------
-- what does engagement + review distribution look like by top category? 
-------------------------------------------------------------------------------
-- active listings, high vs low stakes, listing views, gms coverage, unique visits, conversions, total reviews, review_seen events, review_seen visits, review distrbution by rating
with active_listing_views as (
select
  listing_id,
  price,
  views,
  unique_views,
  purchases,
  gms (?)
  )
, reviews as (
)
, reviews_seen 
