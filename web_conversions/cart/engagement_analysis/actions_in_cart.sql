----------------------------------------------------------------------------------------------------------------
-- What do browsers do in cart?
----------------------------------------------------------------------------------------------------------------
-- SAVE FOR LATER/ REMOVE FROM CART
select
  platform,
  buyer_segment,
  -- new_visitor,
  --browser level metrics
  count(distinct case when event_type in ('cart_view') then browser_id end) as browsers_w_cart,
  count(distinct case when event_type in ('checkout_add_to_saved_for_later') then browser_id end) as browsers_w_save_for_later,
  count(distinct case when event_type in ('cart_listing_removed') then browser_id end) as browsers_w_listing_removed,
  -- event level metrics 
  count(case when event_type in ('cart_view') then visit_id end) as cart_views,
  count(case when event_type in ('checkout_add_to_saved_for_later') then visit_id end) as save_for_laters,
  count(case when event_type in ('cart_listing_removed') then visit_id end) as remove_listings,
  count(distinct case when event_type in ('cart_view') then visit_id end) as visits_w_cart_view,
  count(distinct case when event_type in ('checkout_add_to_saved_for_later') then visit_id end) as visits_w_s4l,
  count(distinct case when event_type in ('cart_listing_removed') then visit_id end) as visits_w_remove_listing,
  -- count(case when event_type in ('cart_view') then visit_id end) as visits_w_cart_view,
  -- count(case when event_type in ('checkout_add_to_saved_for_later') then visit_id end) as visits_w_s4l,
  -- count(case when event_type in ('cart_listing_removed') then visit_id end) as visits_w_remove_listing,
from 
  etsy-data-warehouse-dev.madelinecollins.cart_engagement_browsers
inner join 
  etsy-data-warehouse-prod.weblog.events using (visit_id)
where
  event_type in ('cart_view','checkout_add_to_saved_for_later','cart_listing_removed') 
  and _date >= current_date-30
group by all 
order by 1,2 desc


-- ABANDON CART 
select
  platform,
  -- case when user_id is null or user_id = 0 then 0 else 1 end as buyer_segment,
  new_visitor,
  count(distinct case when cart_adds > 0 then browser_id end) as browser_atc,
  count(distinct case when cart_adds > 0 and converted = 0 then browser_id end) as browser_abandon_cart,
  count(distinct case when cart_adds > 0 and converted > 0 then browser_id end) as browser_converted_carts,
from 
  etsy-data-warehouse-prod.weblog.visits 
where
  platform in ('desktop','mobile_web','boe')
  and _date >= current_date-30
group by all 
order by 1,2 desc 


-- SHOP HOME/ LISTING PAGE FROM CART
/* begin
create or replace temp table all_events as (
select
  platform,
  case when v.user_id is null or v.user_id = 0 then 0 else 1 end as buyer_segment,
  new_visitor,
  visit_id,
  browser_id,
  sequence_number,
  event_type,
  lead(event_type) over (partition by visit_id order by sequence_number) as next_event, 
  lead(event_type) over (partition by visit_id order by sequence_number) as next_sequence_number
from 
    etsy-data-warehouse-prod.weblog.events e
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where
  v._date >= current_date-30
  and platform in ('mobile_web','desktop')
  and page_view = 1 -- only primary pages 
);
end */

select
  platform,
  -- buyer_segment,
  new_visitor,
  event_type,
  next_event,
  count(distinct browser_id) as browsers,
  count(distinct visit_id) as visits,
  count(sequence_number) as events, 
  count(distinct browser_id) / sum(count(distinct browser_id)) over () AS pct_of_browsers,
  count(distinct visit_id) / sum(count(distinct visit_id)) over () AS pct_of_visits,
  count(sequence_number) / sum(count(sequence_number)) over () AS pct_of_events
from 
  etsy-bigquery-adhoc-prod._script8666634f6bde59747fe85ad0f79730c969cda3d6.all_events
where event_type in ('cart_view')
group by all 
qualify rank () over (partition by platform order by count(distinct browser_id) desc) <= 10

