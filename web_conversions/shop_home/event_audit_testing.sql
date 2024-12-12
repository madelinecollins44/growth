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
		date(_partitiontime) as _date, 
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
----Contact seller link

---------------------------------------------------------------------------------------------------------------------------------------------
--SEARCH
---------------------------------------------------------------------------------------------------------------------------------------------
----Search box clicked
----Search box typed in
----Search clicked
----Sort drop down clicked
----Sort drop down option selected (Most recent / Lowest price / Highest price / Custom)

---------------------------------------------------------------------------------------------------------------------------------------------
--BROWSE
---------------------------------------------------------------------------------------------------------------------------------------------
----Listing clicked
----Listing favorited / unfavorited

---------------------------------------------------------------------------------------------------------------------------------------------
--REVIEWS
---------------------------------------------------------------------------------------------------------------------------------------------
----Reviews seen (property to surface how many reviews shop has)
----Pagination
----Review sort drop down option clicked
----Review sort drop down option selected (Most recent / Suggested)

---------------------------------------------------------------------------------------------------------------------------------------------
--OTHER
---------------------------------------------------------------------------------------------------------------------------------------------
----Request custom order clicked
----Contact shop owner clicked
----Sales clicked (property to show how many sales)
----Admirers clicked (property to show how many admirers)

