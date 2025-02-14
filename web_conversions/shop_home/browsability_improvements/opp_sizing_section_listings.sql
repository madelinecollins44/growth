-- need to find visits to shop home from listingp pages (grab listing_id here)
-- look to see if that listing_id is in a section 

-- events
--shop_home_section_select: section_id, num_sections
--neu_favorite_click: click listings
--favorite_toast_notification_shown: see favorite confirmation

------------------------------
-- QUERY TO PULL IN LISTING_ID
------------------------------
-- this is where i look at shop_home views, proceeded by listing views. i grab the listing_id and see if that listing is in a section in that shop. 
with section_info as ( -- gets whether or not a listing is in a section
select
  listing_id,
  shop_id,
  section_id
from etsy-data-warehouse-prod.etsy_shard.listings
where section_id > 0 -- if section_id is 0, it does not have a section_id
)
, events as (
select
  platform,
  visit_id,
  event_type,
  listing_id,
  sequence_number,
  lead(event_type) over (partition by visit_id order by sequence_number) as next_page,
  lead(sequence_number) over (partition by visit_id order by sequence_number) as next_sequence_number,
from 
  etsy-data-warehouse-prod.weblog.events e
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date- 30  
  and page_view =1 -- only primary pages 
  and v.platform in ('boe','mobile_web','desktop')
group by all
)
, sh_from_lp as ( -- how many times did a listing bring a visit to the shop home page? 
select
  platform,
  e.listing_id,
  case when s.listing_id is not null then 1 else 0 end as in_section,
  visit_id,
  count(sequence_number) as views
from 
  events e
left join 
  section_info s 
    on cast(s.listing_id as string)=e.listing_id
where
  event_type in ('view_listing')
  and next_page in ('shop_home')
group by all 
)
select
  platform,
  in_section,
  count(distinct listing_id) as listings,
  count(visit_id) as views,
  count(distinct visit_id) as visits
from 
  sh_from_lp 
group by all 


------------------------------
-- TESTING
------------------------------
-- TEST 1: make sure '%from_page=listing%' is an okay to pull listing_id from pages.
----UPDATE: it does not work, as only 15% of shop home views have from_page=listing in url, 
  -----but 35% of shop home events have a view_listing event before the shop_home event.

  --find share of url: REGEXP_EXTRACT(url, r'listing_id=(\d+)') AS listing_id
select 
  count(sequence_number) as events,
  count(case when url like ('%from_page=listing%') then sequence_number end) as lp_referrers,
  count(case when url like ('%from_page=listing%') then sequence_number end) / count(sequence_number) as share
from etsy-data-warehouse-prod.weblog.events 
where event_type in ('shop_home') and _date >= current_date- 5 
-- events	lp_referrers	share
-- 71354129	11333020	0.15882780938997937

-- check again weblog.visits 
with events as (
select
  visit_id,
  event_type,
  sequence_number,
  lead(event_type) over (partition by visit_id order by sequence_number) as next_page,
  lead(sequence_number) over (partition by visit_id order by sequence_number) as next_sequence_number,
from etsy-data-warehouse-prod.weblog.events 
where _date >= current_date- 5 and page_view =1 
group by all
)
select
  count(case when event_type in ('listing_view') and next_page in ('shop_home') then sequence_number end) as lp_to_sh_events,
  count(case when event_type in ('shop_home') then sequence_number end) as sh,
  count(case when event_type in ('listing_view') and next_page in ('shop_home') then sequence_number end) /  count(case when event_type in ('shop_home') then sequence_number end) as share
from events
