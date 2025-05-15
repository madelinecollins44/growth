----------------------------------------------------------------------------------------------------------------
-- What do browsers do in cart?
----------------------------------------------------------------------------------------------------------------
-- SAVE FOR LATER/ REMOVE FROM CART
with actions as (
select
  visit_id,
  event_type,
  count(sequence_number) as views
from
  etsy-data-warehouse-prod.weblog.events 
where   
event_type in ('cart_view','checkout_add_to_saved_for_later','cart_listing_removed') 
and _date >= current_date-30
group by all
)
select
  platform,
  -- buyer_segment,
  new_visitor,
  --browser level metrics
 coalesce(count(distinct case when event_type in ('cart_view') then browser_id end),0) as browsers_w_cart,
  coalesce(count(distinct case when event_type in ('checkout_add_to_saved_for_later') then browser_id end),0) as browsers_w_save_for_later,
  coalesce(count(distinct case when event_type in ('cart_listing_removed') then browser_id end),0) as browsers_w_listing_removed,
  -- event level metrics 
  coalesce(sum(case when event_type in ('cart_view') then views end),0) as cart_views,
  coalesce(sum(case when event_type in ('checkout_add_to_saved_for_later') then views end),0) as save_for_laters,
  coalesce(sum(case when event_type in ('cart_listing_removed') then views end),0) as remove_listings,
  -- visit level
  coalesce(count(distinct case when event_type in ('cart_view') then visit_id end),0) as visits_w_cart,
  coalesce(count(distinct case when event_type in ('checkout_add_to_saved_for_later') then visit_id end),0) as visits_w_save_for_later,
  coalesce(count(distinct case when event_type in ('cart_listing_removed') then browser_id end),0) as visits_w_remove_listing,
from 
  etsy-data-warehouse-dev.madelinecollins.cart_engagement_browsers
inner join 
  actions using (visit_id)
group by all 
order by 1,2 desc

-- ABANDON CART 

select
  platform,
  -- case when user_id is null or user_id = 0 then 0 else 1 end as buyer_segment,
  new_visitor,
  -- browser metrics
  count(distinct case when cart_adds > 0 then browser_id end) as browser_atc,
  count(distinct case when cart_adds > 0 and converted = 0 then browser_id end) as browser_abandon_cart,
  count(distinct case when cart_adds > 0 and converted > 0 then browser_id end) as browsers_converted_carts,
  -- visit metrics
  count(distinct case when cart_adds > 0 then visit_id end) as visits_atc,
  count(distinct case when cart_adds > 0 and converted = 0 then visit_id end) as visits_abandon_cart,
  count(distinct case when cart_adds > 0 and converted > 0 then visit_id end) as visits_converted_carts,
from 
  etsy-data-warehouse-prod.weblog.visits 
where
  platform in ('desktop','mobile_web','boe')
  and _date >= current_date-30
group by all 
order by 1,2 desc 



-- SHOP HOME/ LISTING PAGE FROM CART
/*begin
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
  buyer_segment,
  -- new_visitor,
  event_type,
  next_event,
  count(distinct browser_id) as browsers,
  count(distinct visit_id) as visits,
  count(sequence_number) as events, 
  count(distinct browser_id) / sum(count(distinct browser_id)) over () AS pct_of_browsers,
  count(distinct visit_id) / sum(count(distinct visit_id)) over () AS pct_of_visits,
  count(sequence_number) / sum(count(sequence_number)) over () AS pct_of_events
from 
  etsy-bigquery-adhoc-prod._script866f860a6b2175616d04ef2e05e6e9b664317020.all_events
where event_type in ('cart_view') and buyer_segment=1
group by all 
qualify rank () over (partition by platform order by count(distinct browser_id) desc) <= 10
