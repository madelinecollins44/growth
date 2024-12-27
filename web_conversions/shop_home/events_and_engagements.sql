---------------------------------------------------------------------------------------------------------------------------------------------
--PAGEVIEWS
---------------------------------------------------------------------------------------------------------------------------------------------
-- what % of shop home pageviews are for an active shop
select 
  event_type,
  count(e.visit_id) as pageviews
from etsy-data-warehouse-prod.weblog.events e
inner join etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where v._date >= current_date-14
  and platform in ('mobile_web','desktop')
  and event_type like ('%shop_home%')
  and page_view = 1
group by all
order by 2 desc
	
-- what % of pageviews are for an active shop home 
select 
  count(e.visit_id) as pageviews,
  count(case when event_type in ('shop_home') then visit_id end) as shop_home_pageviews,
  count(case when event_type in ('shop_home_inactive') then visit_id end) as inactive_shop_home_pageviews,
from etsy-data-warehouse-prod.weblog.events e
inner join etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where v._date >= current_date-14
  and platform in ('mobile_web','desktop')
   and page_view = 1
group by all
order by 2 desc
---------------------------------------------------------------------------------------------------------------------------------------------
--LANDINGS
---------------------------------------------------------------------------------------------------------------------------------------------
-- what % of landings are shop home
select
  count(distinct case when landing_event in ('shop_home') then visit_id end) as shop_home_landings,
  count(distinct visit_id) as total_visits,
  count(distinct case when landing_event in ('shop_home') then visit_id end)/ count(distinct visit_id) as share_of_visits
from etsy-data-warehouse-prod.weblog.visits  
where 
  _date >= current_date-14
  and platform in ('mobile_web','desktop')

-- what other events are associated with a shop home?
select 
  landing_event,
  count(distinct visit_id) as total_visits
from etsy-data-warehouse-prod.weblog.visits  
where _date >= current_date-14
  and platform in ('mobile_web','desktop')
  and landing_event like ('%shop_home%')
group by all
order by 2 desc

-- where do landings from shop home inactive pages come from?
select 
  case 
      when top_channel in ('direct') then 'Direct'
      when top_channel in ('dark') then 'Dark'
      when top_channel in ('internal') then 'Internal'
      when top_channel in ('seo') then 'SEO'
      when top_channel like 'social_%' then 'Non-Paid Social'
      when top_channel like 'email%' then 'Email'
      when top_channel like 'push_%' then 'Push'
      when top_channel in ('us_paid','intl_paid') then
        case
          when (second_channel like '%gpla' or second_channel like '%bing_plas') then 'PLA'
          when (second_channel like '%_ppc' or second_channel like 'admarketplace') then case
          when third_channel like '%_brand' then 'SEM - Brand' else 'SEM - Non-Brand'
          end
      when second_channel='affiliates' then 'Affiliates'
      when (second_channel like 'facebook_disp%' or second_channel like 'pinterest_disp%') then 'Paid Social'
      when second_channel like '%native_display' then 'Display'
      when second_channel in ('us_video','intl_video') then 'Video' else 'Other Paid' end
      else 'Other Non-Paid' 
      end as reporting_channel,
  count(distinct visit_id) as total_visits
from etsy-data-warehouse-prod.weblog.visits  
where _date >= current_date-14
  and platform in ('mobile_web','desktop')
  and landing_event in ('shop_home_inactive')
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
  v._date >= current_date-14
  and v.platform in ('mobile_web','desktop')
  and event_type in (
    'shop_home', 
    'shop_home_announcement_section_seen', -- optional, fires top of page 
    'shop_home_branding_section_seen', -- top of page
    'shop_home_listings_section_seen', -- listing section, middle of page
    'shop_home_listing_grid_seen',-- middle
    'shop_home_reviews_section_top_seen', -- bottom
    'shop_home_reviews_section_seen',-- bottom
    'shop_home_about_section_seen',-- bottom
    'shop_home_policies_section_seen',-- bottom
    'shop_home_updates_section_seen', -- is this optional?
    'shop_home_faqs_section_seen'-- bottom
)
group by all

--clicks 
select 
 event_type,
  count(distinct visit_id) as unique_visits,
  count(visit_id) as pageviews,
from 
  etsy-data-warehouse-prod.weblog.events
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date-14
  and platform in ('mobile_web','desktop')
  and (event_type in (
    'shop_home',
    'shop_home_nav_clicked', --Click on page navigation item
    'shop_home_section_select', --Click on shop section item
    'shop_home_items_pagination', --Click on listings pagination item
    'shop_home_reviews_pagination') --Click on reviews pagination item
  or (event_type in ('view_new_hearts_me') and ref_tag in ('shop_home')) --User clicks on the "X Admirers" link
  or (event_type in ('shop_sold') and ref_tag like ('%shop_home%')) --User clicks on the "X Sales" link
  ) 
group by all

---------------------------------------------------------------------------------------------------------------------------------------------
--HEADER
---------------------------------------------------------------------------------------------------------------------------------------------
----Follow shop / unfollow shop (this is not shop specific-- this is looking at overall shop_home views + favorites / unfavorites. due to this, some visits might be double counted)
select
   -- date(_partitiontime) as _date, 
  beacon.event_name, 
  count(visit_id) as views, 
  count(distinct visit_id) as visits
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
  date(_partitiontime) >= current_date-14
  and beacon.event_source in ('web')
  and ((beacon.event_name in ('shop_home'))
-- looking at favoriting on shop_home page
  or (beacon.event_name in ('favorite_shop', 'remove_favorite_shop')
  and (select value from unnest(beacon.properties.key_value) where key = "source") in ('shop_home_branding')))
group by all

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
		date(_partitiontime) >= current_date-14
	  and (beacon.event_source in ('web')
    and ((beacon.event_name in ('shop_home'))
    and (beacon.event_source in ('web')
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

----Review stars at top of page, what is the distro of ratings across visited shop homes 
with visit_shop_homes as (
select
   -- date(_partitiontime) as _date, 
  beacon.event_name, 
  (select value from unnest(beacon.properties.key_value) where key = "shop_id") as shop_id, 
  count(visit_id) as views, 
  count(distinct visit_id) as visits
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
  date(_partitiontime) >= current_date-14
  and beacon.event_source in ('web')
  and (beacon.event_name in ('shop_home'))
group by all
)
, shop_reviews as (
select
  shop_id,
  count(distinct transaction_id) as transactions,
  count(distinct buyer_user_id) as buyers,
  avg(rating) as average_rating,
  count(case when rating = 0 then transaction_id end) as reviews_w_ratings_of_0,
  count(case when rating = 1 then transaction_id end) as reviews_w_ratings_of_1,
  count(case when rating = 2 then transaction_id end) as reviews_w_ratings_of_2,
  count(case when rating = 3 then transaction_id end) as reviews_w_ratings_of_3,
  count(case when rating = 4 then transaction_id end) as reviews_w_ratings_of_4,
  count(case when rating = 5 then transaction_id end) as reviews_w_ratings_of_5,
  count(case when seller_feedback != '' then transaction_id end) as reviews_w_seller_feedback
from 
  etsy-data-warehouse-prod.etsy_shard.shop_transaction_review
where 
  is_deleted = 0 --  only includes active reviews 
  and language in ('en') -- only english reviews
group by all
)
select
  count(distinct v.shop_id) as unique_shops_visited,
  sum(views) as shop_home_pageviews,
  sum(case when r.shop_id is null then 1 else 0 end) as shops_without_reviews,
  sum(transactions) as total_reviews,
  avg(transactions) as avg_reviews,
  avg(average_rating) as average_rating,
  -- sum(reviews_w_ratings_of_0) as reviews_w_ratings_of_0,
  sum(reviews_w_ratings_of_1) as reviews_w_ratings_of_1,
  sum(reviews_w_ratings_of_2) as reviews_w_ratings_of_2,
  sum(reviews_w_ratings_of_3) as reviews_w_ratings_of_3,
  sum(reviews_w_ratings_of_4) as reviews_w_ratings_of_4,
  sum(reviews_w_ratings_of_5) as reviews_w_ratings_of_5,
  sum(reviews_w_seller_feedback) as reviews_w_seller_feedback,
  -- avg(reviews_w_ratings_of_0) as avg_reviews_w_ratings_of_0,
  avg(reviews_w_ratings_of_1) as avg_reviews_w_ratings_of_1,
  avg(reviews_w_ratings_of_2) as avg_reviews_w_ratings_of_2,
  avg(reviews_w_ratings_of_3) as avg_reviews_w_ratings_of_3,
  avg(reviews_w_ratings_of_4) as avg_reviews_w_ratings_of_4,
  avg(reviews_w_ratings_of_5) as avg_reviews_w_ratings_of_5,
  avg(reviews_w_seller_feedback) as avg_reviews_w_seller_feedback
from 
  visit_shop_homes v
left join 
  shop_reviews r 
    on v.shop_id=cast(r.shop_id as string)
	
----See more description link seen / See more description link clicked

--Seller people link, contact seller link
select 
  case
    when event_type in ('view_profile') then 'seller people link'
    when event_type in ('shop_home_contact_clicked') then 'contact seller link'
    else event_type
  end as header_engagement_type,
  count(distinct visit_id) as unique_visits,
  count(visit_id) as pageviews,
from 
  etsy-data-warehouse-prod.weblog.events
inner join etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date-14
  and ((event_type in ('view_profile') and ref_tag in ('shop_home_header'))
       or event_type in ('shop_home_contact_clicked','shop_home'))
  and platform in ('mobile_web','desktop')
group by all
	
---------------------------------------------------------------------------------------------------------------------------------------------
--SEARCH LISTINGS
---------------------------------------------------------------------------------------------------------------------------------------------
--get sitewide search rate for compare
select
  count(distinct visit_id) as visits,
  count(visit_id) as pageviews,
  count(distinct case when event_type in ('search') then visit_id end) as search_visits,
  count(case when event_type in ('search') then visit_id end) as search_pageviews,
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where   
  platform in ('mobile_web','desktop')
  -- event_type in ('search')
  -- and platform in ('mobile_web','desktop')
  and v._date >= current_date-14

--searhc listings
select
   -- date(_partitiontime) as _date, 
  beacon.event_name, 
  count(visit_id) as views, 
  count(distinct visit_id) as visits
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
  date(_partitiontime) >= current_date-14
  and (beacon.event_source in ('web')
  and beacon.event_name in (
        'shop_home', -- primary page 
        'shop_home_search_input_focused', --Search box clicked
        'shop_home_search_input_changed', --Search box typed in
        'shop_home_search_items', --Search clicked
        'shop_home_dropdown_open', --Sort drop down clicked
       'shop_home_dropdown_engagement') --Sort drop down option selected (Most recent / Lowest price / Highest price / Custom)
group by all

-- search filtering 
select 
  -- date(_partitiontime) as _date, 
  beacon.event_name, 
  (select value from unnest(beacon.properties.key_value) where key = "sort_param") as sort_param, 
  count(visit_id) as views, 
  count(distinct visit_id) as visits
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
  date(_partitiontime) >= current_date-14
  and (beacon.event_source in ('web')
  and beacon.event_name in ('shop_home_dropdown_engagement') --Sort drop down option selected (Most recent / Lowest price / Highest price / Custom)
group by all

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
  _date >= current_date-14
  and referring_page_event like ('%shop_home%')
 and platform in ('mobile_web','desktop')
group by all 


----Listing favorited / unfavorited
select
  -- date(_partitiontime) as _date, 
  case 
    when beacon.event_name in ('neu_favorite_click') and (select value from unnest(beacon.properties.key_value) where key = "is_add") in ('true') then 'favorited listing'
    when beacon.event_name in ('neu_favorite_click') and (select value from unnest(beacon.properties.key_value) where key = "is_add") in ('false') then 'unfavorited listing'
    else beacon.event_name 
  end as event_name,
  count(visit_id) as views, 
  count(distinct visit_id) as visits
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
  date(_partitiontime) >= current_date-14
  and (beacon.event_source in ('web')
  and ((beacon.event_name in ('shop_home','favorite_toast_notification_shown','shop_home_listings_section_seen'))
  -- looking at favoriting on shop_home page
  or (beacon.event_name in ('neu_favorite_click')
   and (select value from unnest(beacon.properties.key_value) where key = "page_type") in ('shop')))
group by all

---------------------------------------------------------------------------------------------------------------------------------------------
--REVIEWS
---------------------------------------------------------------------------------------------------------------------------------------------
-- review events
select
   -- date(_partitiontime) as _date, 
  beacon.event_name, 
  count(visit_id) as views, 
  count(distinct visit_id) as visits
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
  date(_partitiontime) >= current_date-14
  and (beacon.event_source in ('web')
  and beacon.event_name in (
        'shop_home', -- primary page 
        'shop_home_reviews_section_top_seen', --Top of reviews section seen
        'shop_home_reviews_section_seen', --Middle of reviews section seen
        'shop_home_reviews_pagination', --Click on reviews pagination item
        'sort_reviews_menu_opened', --Sort drop down option selected (Most recent / Lowest price / Highest price / Custom)
        'sort_reviews', --Select review sort option
        'filter_reviews_by_keyword_shop_home') --Load reviews with keyword filter
group by all

--what page do they typically see in reviews? 
select
   -- date(_partitiontime) as _date, 
  beacon.event_name, 
  (select value from unnest(beacon.properties.key_value) where key = "page") as sort_param, 
  count(visit_id) as views, 
  count(distinct visit_id) as visits
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
  date(_partitiontime) >= current_date-14
  and (beacon.event_source in ('web')
  and beacon.event_name in ('shop_home_reviews_pagination') --Click on reviews pagination item
group by all

-- Review sort drop down option selected (Most recent / Suggested)
select 
  -- date(_partitiontime) as _date, 
  beacon.event_name, 
  (select value from unnest(beacon.properties.key_value) where key = "sort_selected") as sort_param, 
  count(visit_id) as views, 
  count(distinct visit_id) as visits
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
where
  date(_partitiontime) >= current_date-14
 and (beacon.event_source in ('web')
  and beacon.event_name in ('sort_reviews_menu_opened', -- open sorting menu
                            'sort_reviews') --Sort drop down option clicked (Most recent / Lowest price / Highest price / Custom)
group by all

---------------------------------------------------------------------------------------------------------------------------------------------
--OTHER
---------------------------------------------------------------------------------------------------------------------------------------------
----Request custom order clicked
----Contact shop owner clicked
----Sales clicked (property to show how many sales)
----Admirers clicked (property to show how many admirers)

-- How many shops opted in to all of the optional fields?
create or replace table etsy-data-warehouse-dev.madelinecollins.shop_basics as (
select
  basics.shop_id,
  count(distinct case when shop_data.branding_option != 0 then basics.shop_id end) as branding_banner, -- can we confirm 0 means this shop does not have branding? 
  count(distinct case when shop_data.message != ""  then basics.shop_id end) as annoucement, 
  count(distinct case when sections.shop_id is not null then basics.shop_id end) as shop_sections,
  count(distinct case when abt.shop_id is not null then basics.shop_id end) as about_section,
  count(distinct case when faq.shop_id is not null then basics.shop_id end) as faq_section,
  count(distinct case when share_items.shop_id is not null then basics.shop_id end) as updates,
  count(distinct case when personal_details.shop_id is not null then basics.shop_id end) as seller_details,
  count(distinct case when settings.name = 'machine_translation' and settings.value = 'off' then basics.shop_id end) as machine_translation,
  count(distinct case when settings.name = 'custom_orders_opt_in' and settings.value = 't' then basics.shop_id end) as accepts_custom_orders,
  count(distinct case when settings.name = 'hide_shop_home_page_sold_items' and settings.value = 'f' then basics.shop_id end) as show_sold_items, -- confirm that false means these are shown 
  count(distinct case when promoted_offer.shop_id is not null then basics.shop_id end) as offers_active_shop_coupon 
from 
  (select * from etsy-data-warehouse-prod.rollups.seller_basics where active_seller_status = 1) basics -- only looks at active shops
left join 
  (select * from etsy-data-warehouse-prod.etsy_shard.shop_data where status in ('active')) shop_data using (shop_id) -- only active shops 
left join 
    etsy-data-warehouse-prod.etsy_shard.shop_sections sections 
      on basics.shop_id=sections.shop_id
left join 
  (select 
    distinct shop_id 
  from 
    etsy-data-warehouse-prod.etsy_shard.shop_about 
  where 
    status in ('active')
    and not (coalesce(story, '') = '' and coalesce(story_headline, '') = '')
    ) abt  -- excludes inactive shops w/o text 
      on basics.shop_id=abt.shop_id
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_frequently_asked_questions faq
    on basics.shop_id=faq.shop_id
left join 
  (select * from etsy-data-warehouse-prod.etsy_shard.shop_share_items where is_deleted <> 1) share_items -- only looks at shops that currently have updates
    on basics.shop_id=share_items.shop_id
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_seller_personal_details personal_details -- what does details_id mean here? 
    on basics.shop_id=personal_details.shop_id
left join 
  etsy-data-warehouse-prod.etsy_shard.shop_settings settings
    on basics.shop_id=settings.shop_id
left join 
  (select * from etsy-data-warehouse-prod.etsy_shard.seller_marketing_promoted_offer where is_active = 1) promoted_offer
    on basics.shop_id=promoted_offer.shop_id
group by all);


-- % of shops opted into each elements 
select 
  count(distinct shop_id) as total_active_shops,
  sum(branding_banner) as branding_banner, 
  sum(annoucement) as annoucement,
  sum(shop_sections) as shop_sections,
  sum(about_section) as about_section,
  sum(faq_section) as faq_section,
  sum(updates) as updates,
  sum(seller_details) as seller_details,
  sum(machine_translation) as machine_translation,
  sum(accepts_custom_orders) as accepts_custom_orders,
  sum(show_sold_items) as show_sold_items,
  sum(offers_active_shop_coupon) as offers_active_shop_coupon
from 
  etsy-data-warehouse-dev.madelinecollins.shop_basics 
group by all
	
-- distro of elements opted into 
with shop_counts as (
select 
  shop_id,
  sum(branding_banner+annoucement+shop_sections+about_section+faq_section+updates+seller_details+machine_translation+accepts_custom_orders+show_sold_items+offers_active_shop_coupon) as total_elements_opted_into
from 
  etsy-data-warehouse-dev.madelinecollins.shop_basics 
group by all)
select
  total_elements_opted_into,
  count(distinct shop_id)
from shop_counts
group by all order by 1 desc
