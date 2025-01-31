----------------------------------------------------------------------------------------------------------------
--How often do sellers launch new items? What % of sellers have added a new item in the last 30 days?
----------------------------------------------------------------------------------------------------------------

with active_shops as (
)
, visited_shops
, listings 
  case when create_date >= current_date-30 then 1 else 0 end as new_item, cont(listing_id) as listings
