---------------------------------------------------------------------------------------------------------------------------------------------
--LANDINGS
---------------------------------------------------------------------------------------------------------------------------------------------
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
----Follow shop / unfollow shop
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
-- looking at favoriting on shop_home page
	or (beacon.event_name in ('favorite_shop', 'remove_favorite_shop')
        and (select value from unnest(beacon.properties.key_value) where key = "source") in ('shop_home_branding')))
group by all

----Review stars at top of page
----See more description link seen / See more description link clicked
----Seller people link
shop_home_seller_people_link_click
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
----Listing favorited / unfavorited

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

