---------------------------------------------------------------------------------------------------------------------------------------------
--LANDINGS
---------------------------------------------------------------------------------------------------------------------------------------------
select
  count(distinct case when landing_event in ('shop_home') then visit_id end) as shop_home_landings,
  count(distinct visit_id) as total_visits,
  count(distinct case when landing_event in ('shop_home') then visit_id end)/ count(distinct visit_id) as share_of_visits
from etsy-data-warehouse-prod.weblog.visits  
where _date >= current_date-30

select 
  landing_event,
  count(distinct visit_id) as total_visits
from etsy-data-warehouse-prod.weblog.visits  
where _date >= current_date-30
and landing_event like ('%shop_home%')
group by all
order by 2 desc

---------------------------------------------------------------------------------------------------------------------------------------------
--NAVIGATION
---------------------------------------------------------------------------------------------------------------------------------------------
--scroll depth
select
  event_type,
  count(distinct visit_id) as visits
from 
  etsy-data-warehouse-prod.weblog.events e  
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date-30
  and v.platform in ('mobile_web','desktop')
  and event_type in (
    'shop_home_announcement_section_seen', -- optional, fires top of page 
    'shop_home_branding_section_seen', -- top of page
    'shop_home_listings_section_seen', -- listing section, middle of page
    'shop_home_listing_grid_seen'-- middle
    'shop_home_reviews_section_top_seen', -- bottom
    'shop_home_reviews_section_seen',-- bottom
    'shop_home_about_section_seen',-- bottom
    'shop_home_policies_section_seen',-- bottom
    'shop_home_updates_section_seen', -- is this optional?
    'shop_home_faqs_section_seen',-- bottom
)
group by all

--clicks 

---------------------------------------------------------------------------------------------------------------------------------------------
--HEADER
---------------------------------------------------------------------------------------------------------------------------------------------
----How many shops have announcements? 
----Follow shop / unfollow shop (this is not shop specific-- this is looking at overall shop_home views + favorites / unfavorites. due to this, some visits might be double counted)
select
   -- date(_partitiontime) as _date, 
  beacon.event_name, 
  count(visit_id) as views, 
  count(distinct visit_id) as visits
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
  date(_partitiontime) >= current_date-7
  and ((beacon.event_name in ('shop_home'))
  -- and (beacon.event_name in ('shop_home','favorite_shop_added','favorite_shop_removed') --favorite_shop_removed is missing
-- looking at favoriting on shop_home page
  or (beacon.event_name in ('favorite_shop', 'remove_favorite_shop')
  and (select value from unnest(beacon.properties.key_value) where key = "source") in ('shop_home_branding')))
group by all
\\-- event_name	views	visits
\\-- favorite_shop	545831	398963
\\-- remove_favorite_shop	78189	27421
\\-- shop_home	124575822	50697982

\\-- event_name	views	visits
\\-- favorite_shop_added	1993813	1545537
\\-- shop_home	124575822	50697982

--this is favoriting at the shop_id level 
with get_shop_ids as (
select
  beacon.event_name,
  (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id,
  visit_id, 
  count(visit_id) as views
from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where
		date(_partitiontime) >= current_date-7
    and ((beacon.event_name in ('shop_home'))
    -- looking at favoriting on shop_home page
	    or (beacon.event_name in ('favorite_shop', 'remove_favorite_shop')
      and (select value from unnest(beacon.properties.key_value) where key = "source") in ('shop_home_branding')))
group by all
)
, shop_metrics as (
select
  shop_id,
  sum(case when event_name in ('shop_home') then views end) as shop_home_pageviews,
  sum(case when event_name in ('favorite_shop') then views end) as favorite_events,
  sum(case when event_name in ('remove_favorite_shop') then views end) as unfavorite_events,
  count(distinct case when event_name in ('shop_home') then views end) as shop_home_visits,
  count(distinct case when event_name in ('favorite_shop') then views end) as favorite_visits,
  count(distinct case when event_name in ('remove_favorite_shop') then views end) as unfavorite_visits
from get_shop_ids
group by all 
)
select
  count(distinct shop_id) as unique_shops,
  sum(shop_home_pageviews) as shop_home_pageviews,
  sum(favorite_events) as favorite_events,
  sum(unfavorite_events) as unfavorite_events,
from shop_metrics

----Review stars at top of page
----See more description link seen / See more description link clicked

----Seller people link
shop_home_seller_people_link_click
property: ref = shop_home_header

----Contact seller link
shop_home_contact_clicked

---------------------------------------------------------------------------------------------------------------------------------------------
--SEARCH LISTINGS
---------------------------------------------------------------------------------------------------------------------------------------------
----Search box clicked
shop_home_search_input_focused

----Search box typed in
shop_home_search_input_changed

----Search clicked
shop_home_search_items

----Sort drop down clicked
shop_home_dropdown_open

----Sort drop down option selected (Most recent / Lowest price / Highest price / Custom)
shop_home_dropdown_engagement 
property: sort_param

---------------------------------------------------------------------------------------------------------------------------------------------
--BROWSE
---------------------------------------------------------------------------------------------------------------------------------------------
----Listing clicked
select
  referring_page_event,
  -- REGEXP_EXTRACT(ref_tag, r'^([^_]+_[^_]+_[^_]+)') AS ref_tag_main, -- get the root of the ref tag
  count(listing_id) as listing_views,
  count(case when purchased_after_view > 0 then listing_id end) as purchased_listings
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
  and referring_page_event like ('%shop_home%')
group by all 

----Listing favorited / unfavorited
select
  -- date(_partitiontime) as _date, 
  beacon.event_name, 
  (select value from unnest(beacon.properties.key_value) where key = "is_add"), -- will say adding or unadding
  count(visit_id) as views, 
  count(distinct visit_id) as visits
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
  date(_partitiontime) >= current_date-7
  and ((beacon.event_name in ('shop_home','favorite_toast_notification_shown'))
  -- looking at favoriting on shop_home page
  or (beacon.event_name in ('neu_favorite_click')
   and (select value from unnest(beacon.properties.key_value) where key = "page_type") in ('shop')))
group by all

---------------------------------------------------------------------------------------------------------------------------------------------
--REVIEWS
---------------------------------------------------------------------------------------------------------------------------------------------
----Reviews seen (property to surface how many reviews shop has)
shop_home_reviews_section_top_seen, shop_home_reviews_section_seen (see more than 50%) 
property: shop_review_count
	
----Pagination
shop_home_reviews_pagination
property: page

----Review sort drop down option clicked
sort_reviews_menu_opened

----Review sort drop down option selected (Most recent / Suggested)
sort_reviews
property: sort_selected

---------------------------------------------------------------------------------------------------------------------------------------------
--OTHER
---------------------------------------------------------------------------------------------------------------------------------------------
----Request custom order clicked
----Contact shop owner clicked
----Sales clicked (property to show how many sales)
----Admirers clicked (property to show how many admirers)

