----------------------------------------------------------------------------------------------------------------
-- What do browsers do in cart?
----------------------------------------------------------------------------------------------------------------
-- save for later, remove from cart 
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


-- abandon cart 
select
  platform,
  -- case when user_id is null or user_id = 0 then 0 else 1 end as buyer_segment,
  -- new_visitor,
  count(distinct case when cart_adds > 0 then browser_id end) as browser_atc,
  count(distinct case when cart_adds > 0 and converted = 0 then browser_id end) as browser_abandon_cart,
  count(distinct case when cart_adds > 0 and converted > 0 then browser_id end) as browser_converted_carts,
from 
  etsy-data-warehouse-prod.weblog.visits 
where
  platform in ('desktop','mobile_web')
  and _date >= current_date-30
group by all 
