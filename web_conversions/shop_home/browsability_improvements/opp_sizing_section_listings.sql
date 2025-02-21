-- need to find visits to shop home from listingp pages (grab listing_id here)
-- look to see if that listing_id is in a section 

-- events
--shop_home_section_select: section_id, num_sections
--neu_favorite_click: click listings
--favorite_toast_notification_shown: see favorite confirmation

------------------------------------------------------------------------------------------
-- SEE HOW MANY LISTING VIEWS GO TO SHOP HOME PAGE
------------------------------------------------------------------------------------------
with events as (
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
select
  platform,
  count(distinct case when event_type in ('view_listing') then visit_id end) as visits_w_listing_view,
  count(case when event_type in ('view_listing') then visit_id end) as listing_views,
  count(distinct case when event_type in ('shop_home') then visit_id end) as visits_w_shop_home,
  count(case when event_type in ('shop_home') then visit_id end) as shop_home_views,
  count(distinct case when event_type in ('shop_home') and next_page in ('shop_home') then visit_id end) as visits_lp_to_sh,
  count(case when event_type in ('shop_home') and next_page in ('shop_home') then visit_id end) as views_lp_to_sh,
from 
  events e
group by all 

------------------------------
-- QUERY TO PULL IN LISTING_ID
------------------------------
-- this is where i look at shop_home views, proceeded by listing views. i grab the listing_id and see if that listing is in a section in that shop. 
with section_info as ( -- gets whether or not a listing is in a section
select
  l.listing_id,
  l.shop_id,
  section_id
from 
  etsy-data-warehouse-prod.etsy_shard.listings l
inner join 
  etsy-data-warehouse-prod.rollups.active_listing_basics b using (listing_id) -- only looking at active listings
where 
  section_id > 0 -- if section_id is 0, it is not in a section
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

--overall counts to compare
select
  platform,
  count(case when event_type in ('shop_home') then visit_id end) as shop_home_views,
  count(distinct case when event_type in ('shop_home') then visit_id end) as shop_home_visits,
  count(case when event_type in ('view_listing') then visit_id end) as view_listing_views,
  count(distinct case when event_type in ('view_listing') then visit_id end) as view_listing_visits,
from 
  etsy-data-warehouse-prod.weblog.events e
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date- 30  
  and v.platform in ('boe','mobile_web','desktop')
group by all


-- traffic to shop home from listing page by platform 
  with events as (
select
  platform,
  visit_id,
  event_type,
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
select
  platform,
  count(case when event_type in ('view_listing') and next_page in ('shop_home') then sequence_number end) as lp_to_sh_events,
  count(case when event_type in ('shop_home') then sequence_number end) as sh,
  count(case when event_type in ('view_listing') and next_page in ('shop_home') then sequence_number end) /  count(case when event_type in ('shop_home') then sequence_number end) as share
from events
group by all 
  
------------------------------
-- TESTING
------------------------------
-- TEST 1: make sure '%from_page=listing%' is an okay to pull listing_id from pages.
----UPDATE: it does not work, as only 15% of shop home views have from_page=listing in url, but 35% of shop home events have a view_listing event before the shop_home event.
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


--TEST 2: looking at listings without a section, making sure they are still acive on site
select
  l.listing_id,
  shop_name,
  l.title,
  l.shop_id,
  section_id
from etsy-data-warehouse-prod.etsy_shard.listings l
inner join etsy-data-warehouse-prod.rollups.active_listing_basics b using (listing_id) -- only looking at active listings
inner join etsy-data-warehouse-prod.rollups.seller_basics sb on l.shop_id=sb.shop_id
where section_id = 0 -- if section_id is 0, it does not have a section_id
limit 10
-- listing_id	shop_name	title	shop_id	section_id
-- 1837978463	GlenbrookeTreasures	Anita Goodesign Embroidery Designs CD My True Love Gave to Me NEW	7589081	0
-- 910467236	EssenceOfMotherEarth	"Whipped organic body butter with rose, frankincense  & bergamot essential oil in glass jar"	25707734	0
-- 1745035303	JmaccessoriesByQ	Green Rose and heart keychain/ purse charm	50506279	0
-- 1377685590	SweetPickinsSTL	"Repurposed Decorated Bottlebrush Tree, vintage Mercury Glass beads, Valentine&#39;s day, pink"	23203336	0
-- 1865767569	ArtiqueAve	"Graphic Deer Hoodie, Woodland Animal Sweatshirt, Nature Lover Gift, Deer Lover Apparel, Outdoor Enthusiast Clothing, Wildlife Graphic Jumper"	48079604	0
-- 1309370497	DOnlyBeads	"hand made glass beaded necklace set, quality made costume jewelry, mom or grandmother present,friend or co-worker gift, appreciation token"	30392375	0
-- 1863045427	ASuperStore	Personalized Made in Brazil Mug Customizable Coffee Cup Gift for Brazilian Pride Souvenir Custom Text Mug 11oz 15oz	37386550	0
-- 1863884747	ASuperStore	"Funny Sloth Mug, Still Trying to Decide Mug, Sarcastic Gift, Cute Animal Design Mug, Humorous Coffee Cup, Hilarious Morning Mug"	37386550	0
-- 1864859869	ASuperStore	"Inspirational Quote Coffee Mug, Dream It Do It Mug, Motivational Mug for Office, Gift for Coworker, Positive Vibes Ceramic Mug, Encouraging"	37386550	0
-- 281284308	JWalkerDesigns2013	Beautiful ladies occasion straw hat decorated with a handmade pale pink ribbon rose.	8080285	0

--TEST 3: checking across visit_id level
with events as (
select
  platform,
  visit_id,
  listing_id,
  event_type,
  sequence_number,
  lead(event_type) over (partition by visit_id order by sequence_number) as next_page,
  lead(sequence_number) over (partition by visit_id order by sequence_number) as next_sequence_number,
from 
  etsy-data-warehouse-prod.weblog.events e
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date- 3  
  and page_view =1 -- only primary pages 
  and v.platform in ('boe','mobile_web','desktop')
group by all
)
select visit_id, count(*) 
from events 
where event_type in ('view_listing')
and next_page in ('shop_home')
group by all 
having count(*) = 30
order by 2 desc limit 5
-- visit_id	f0_
-- djXYD-bcTk2sNENO4lVzkg.1739297436668.1	30
-- YO4-lXGLXRXQjK7LJfEh_B13bBuW.1739356360234.1	30
-- ieu_VBIKNYC7Y82RgmGy1B-MM9oM.1739298322408.4	30
-- 0NXDaSOi74uda9BuBelcKl2HV3Dy.1739380838696.1	30
-- ZyYMFwGfTiq4L64WWRmjlQ.1739467389580.3	30

--check visit 
select
  platform,
  listing_id,
  in_section,
  count(distinct listing_id) as listings,
  count(visit_id) as views,
  count(distinct visit_id) as visits
from 
  sh_from_lp 
where visit_id in ('IMdK8hMNrmiQQzi8EzIuh2g36oAZ.1739265527830.1')
group by all 
-- platform	listing_id	in_section	listings	views	visits
-- desktop	1701858566	1	1	1	1
-- desktop	1664956998	1	1	1	1
-- desktop	1834257731	1	1	1	1
-- desktop	1339716204	1	1	1	1
-- desktop	1298294529	0	1	1	1
-- desktop	1812222000	1	1	1	1
-- desktop	1463566268	1	1	1	1
-- desktop	1837665446	0	1	1	1
-- desktop	1808937916	1	1	1	1
-- desktop	1668370163	1	1	1	1
-- desktop	1781403946	1	1	1	1
-- desktop	1867715547	0	1	1	1
-- desktop	1850587718	1	1	1	1
-- desktop	1779403169	1	1	1	1
