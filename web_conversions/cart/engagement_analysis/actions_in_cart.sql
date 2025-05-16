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
  lead(sequence_number) over (partition by visit_id order by sequence_number) as next_sequence_number
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
  etsy-bigquery-adhoc-prod._script44fdc4e00c86c436735e8a34b292f6b7b72deee6.all_events
where event_type in ('cart_view') --and new_visitor=1
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


--TEST 2: next page testing
select visit_id, count(*) from etsy-bigquery-adhoc-prod._script44fdc4e00c86c436735e8a34b292f6b7b72deee6.all_events where event_type in ('cart_view') group by all having count(*) <= 50  order by 2 desc limit 5
/*
visit_id	f0_
-hSUN-84DAunVOaR9OLe02dcOw1p.1745335092541.1	50
LJSfiOFLoLKLjYKUb1qidnES2OjH.1745973824793.1	50
e4ewbdMF0tHZEZVyrcVg17vPJmu1.1745042789778.2	50
XR9qHUmLpQXP0G2-NQYOdNqJeQlO.1746348550213.4	50
QdM1B43j33Ttr0-F75NxJ0aVTl8B.1747060752742.1	50

-- with carts 
visit_id	f0_
RK2PPZJ4aupEcd7S2XCr_sim4o8f.1746704899970.1	50
UfE9_9J8FLjtqrJ1pE2Bx1JAFhDO.1745166896125.1	50
qU2Wa0xonVO9HWDuGnahkRkF3f3d.1747070733646.1	50
5R7ajEoOWhS7tCvxK2Qd3JPuWt1K.1745378877983.1	50
GWbq98I6CGnFFc5IZhfDanOQ4cAp.1747246182005.3	50
*/
select event_type, sequence_number from  etsy-data-warehouse-prod.weblog.events where page_view =1 and visit_id in ('RK2PPZJ4aupEcd7S2XCr_sim4o8f.1746704899970.1') order by sequence_number asc
/*
event_type	sequence_number
view_listing	3
view_listing	103
view_listing	140
view_listing	209
view_listing	272
view_listing	360
view_listing	390
view_listing	471
view_listing	567
search	615
view_listing	650
search	708


event_type	sequence_number
login_view	0
home	4
home	21
search	47
async_listings_search	85
async_listings_search	121
async_listings_search	155
async_listings_search	190
async_listings_search	217
async_listings_search	260
async_listings_search	292
async_listings_search	335
async_listings_search	375
shop_home	406
view_listing	432
view_listing	460
view_listing	490
view_listing	518
view_listing	549
view_listing	594
view_listing	628
view_listing	631
view_listing	690
view_listing	718
view_listing	747
view_listing	779
view_listing	811
view_listing	842
view_listing	877
view_listing	906
view_listing	936
view_listing	964
view_listing	996
view_listing	1024
view_listing	1065
view_listing	1075
view_listing	1124
view_listing	1125
view_listing	1186
view_listing	1214
view_listing	1248
view_listing	1255
view_listing	1309
view_listing	1337
view_listing	1373
view_listing	1401
view_listing	1438
view_listing	1466
view_listing	1498
view_listing	1527
view_listing	1562
view_listing	1590
view_listing	1625
view_listing	1654
view_listing	1686
view_listing	1714
view_listing	1746
view_listing	1776
view_listing	1804
view_listing	1833
view_listing	1867
view_listing	1897
view_listing	1928
view_listing	1963
view_listing	2783
cart_view	3029
cart_view	3052
cart_view	3088
cart_view	3141
cart_view	3224
cart_view	3332
cart_view	3488
cart_view	3677
cart_view	3938
cart_view	4248
cart_view	4587
cart_view	4926
cart_view	5260
cart_view	5592
cart_view	5932
cart_view	6266
cart_view	6605
*/

select
  platform,
  -- buyer_segment,
  -- new_visitor,
  event_type,
  sequence_number,
  next_event,
  count(distinct browser_id) as browsers,
  count(distinct visit_id) as visits,
  count(sequence_number) as events, 
  count(distinct browser_id) / sum(count(distinct browser_id)) over () AS pct_of_browsers,
  count(distinct visit_id) / sum(count(distinct visit_id)) over () AS pct_of_visits,
  count(sequence_number) / sum(count(sequence_number)) over () AS pct_of_events
from 
  etsy-bigquery-adhoc-prod._script44fdc4e00c86c436735e8a34b292f6b7b72deee6.all_events
where visit_id in ('RK2PPZJ4aupEcd7S2XCr_sim4o8f.1746704899970.1') and event_type in ('cart_view')
group by all 
order by 3 asc

/*
platform	event_type	sequence_number	next_event	browsers	visits	events	pct_of_browsers	pct_of_visits	pct_of_events
mobile_web	view_listing	3	view_listing	1	1	1	0.02	0.02	0.02
mobile_web	view_listing	103	view_listing	1	1	1	0.02	0.02	0.02
mobile_web	view_listing	140	view_listing	1	1	1	0.02	0.02	0.02
mobile_web	view_listing	209	view_listing	1	1	1	0.02	0.02	0.02
mobile_web	view_listing	272	view_listing	1	1	1	0.02	0.02	0.02
mobile_web	view_listing	360	view_listing	1	1	1	0.02	0.02	0.02
mobile_web	view_listing	390	view_listing	1	1	1	0.02	0.02	0.02
mobile_web	view_listing	471	view_listing	1	1	1	0.02	0.02	0.02
mobile_web	view_listing	567	search	1	1	1	0.02	0.02	0.02
mobile_web	search	615	view_listing	1	1	1	0.02	0.02	0.02
mobile_web	view_listing	650	search	1	1	1	0.02	0.02	0.02
mobile_web	search	708	view_listing	1	1	1	0.02	0.02	0.02
mobile_web	view_listing	747	market	1	1	1	0.02	0.02	0.02
mobile_web	market	813	view_listing	1	1	1	0.02	0.02	0.02
mobile_web	view_listing	831	market	1	1	1	0.02	0.02	0.02
mobile_web	market	877	view_listing	1	1	1	0.02	0.02	0.02
mobile_web	view_listing	906	market	1	1	1	0.02	0.02	0.02
mobile_web	market	939	view_listing	1	1	1	0.02	0.02	0.02
mobile_web	view_listing	987	market	1	1	1	0.02	0.02	0.02
mobile_web	market	1043	view_listing	1	1	1	0.02	0.02	0.02

platform	event_type	sequence_number	next_event	browsers	visits	events	pct_of_browsers	pct_of_visits	pct_of_events
desktop	cart_view	3029	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	3052	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	3088	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	3141	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	3224	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	3332	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	3488	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	3677	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	3938	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	4248	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	4587	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	4926	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	5260	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	5592	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	5932	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	6266	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	6605	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	6938	cart_view	1	1	1	0.02	0.02	0.02
desktop	cart_view	7274	cart_view	1	1	1	0.02	0.02	0.02

*/
