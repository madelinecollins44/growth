----------------------------------------------------------------------------------------------------------------
-- What do browsers do in cart?
----------------------------------------------------------------------------------------------------------------
select
  --browser level metrics
  count(distinct case when event_type in ('cart_view') then browser_id end) as browsers_w_cart,
  count(distinct case when event_type in ('checkout_add_to_saved_for_later') then browser_id end) as browsers_w_save_for_later,
  count(distinct case when event_type in ('cart_listing_removed') then browser_id end) as browsers_w_listing_removed,
  -- event level metrics 
  count(case when event_type in ('cart_view') then sequence_number end) as cart_views,
  count(case when event_type in ('checkout_add_to_saved_for_later') then sequence_number end) as save_for_laters,
  count(case when event_type in ('cart_listing_removed') then sequence_number end) as remove_listings,
from 
  etsy-data-warehouse-dev.madelinecollins.cart_engagement_browsers
inner join 
  etsy-data-warehouse-prod.weblog.events using (visit_id)
