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




------------------------------------------------------------
-- TESTING
------------------------------------------------------------
-- TEST 1: make sure browsers make sense w actions im seeing
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
  browser_id,
  event_type,
  count(distinct visit_id) as total_visits
from 
  etsy-data-warehouse-dev.madelinecollins.cart_engagement_browsers
inner join 
  actions using (visit_id)
where browser_id in ('45AEB87784B1414E8CAB89902298')
group by all 
order by 2 desc
-- limit 5
/*
browser_id	total_visits
U1noQBuNFhhPXJ4ZV7ac-yWFLKcu	454
gZ8YLYq1TX-U639xR8Vn2A	431
BifP-pYrRZOHvhWxnMGPpQ	413
m-9o4mMJDCW0GpWRwv-P1OISAsv6	412
FTWDseRnnMZyXwTMlsKPeLvdH_v2	403
*/


/*
browser_id	event_type	total_visits
U1noQBuNFhhPXJ4ZV7ac-yWFLKcu	cart_view	454
U1noQBuNFhhPXJ4ZV7ac-yWFLKcu	cart_listing_removed	2

browser_id	event_type	total_visits
FTWDseRnnMZyXwTMlsKPeLvdH_v2	cart_view	403
FTWDseRnnMZyXwTMlsKPeLvdH_v2	cart_listing_removed	3
*/

/* 
browser_id	visits_w_cart	visits_w_save_for_later	visits_w_remove_listing
HwteIRE6iw6FLWaDqWdUKOBIJIUm	279	198	1
E58EFFFAB0074871BEC365FF6C66	230	159	1
eLaAjK9zFVXa91weVTmVtTJ0nHjU	319	155	1
7260EA7A4A25471EB4AFC45D5FFB	248	155	1
45AEB87784B1414E8CAB89902298	285	127	1

browser_id	event_type	total_visits
45AEB87784B1414E8CAB89902298	checkout_add_to_saved_for_later	127
45AEB87784B1414E8CAB89902298	cart_view	285
45AEB87784B1414E8CAB89902298	cart_listing_removed	200
*/

select event_type, count(distinct visit_id) from etsy-data-warehouse-prod.weblog.events 
where event_type in ('cart_view','checkout_add_to_saved_for_later','cart_listing_removed') 
and _date >= current_date-30
and REGEXP_EXTRACT(visit_id, r'^([^\.]+)') in ('U1noQBuNFhhPXJ4ZV7ac-yWFLKcu')
group by all 
