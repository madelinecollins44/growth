--overall traffic 
select
  platform,
  count(distinct visit_id) as visits,
  count(distinct case when converted > 0 then visit_id end) as visits
from 
  etsy-data-warehouse-prod.weblog.visits v
where 
  v._date >= current_date-30
  and v.platform in ('mobile_web','desktop')
group by all

--traffic to shop home/ listing grid  
select
  platform,
  event_type,
  count(distinct visit_id) as visits,
  count(distinct case when converted > 0 then visit_id end) as converted_visits
from 
  etsy-data-warehouse-prod.weblog.events e  
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date-30
  and v.platform in ('mobile_web','desktop')
  and event_type in ('shop_home', 'shop_home_listing_grid_seen','shop_home_listings_section_seen')
group by all

----browsers that do this at least once 
--traffic to shop home/ listing grid  
with agg as (
select
  platform,
  event_type,
  browser_id,
  max(case when event_type in ('shop_home') then 1 else 0 end) as shop_home_visit,
  max(case when event_type in ('shop_home_listing_grid_seen') then 1 else 0 end) as grid_visit,
  max(case when event_type in ('shop_home_listings_section_seen') then 1 else 0 end) as section_visit,
from 
  etsy-data-warehouse-prod.weblog.events e  
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date-30
  and v.platform in ('mobile_web','desktop')
  and event_type in ('shop_home', 'shop_home_listing_grid_seen','shop_home_listings_section_seen')
group by all
)
select
  platform,
  count(distinct browser_id) as browsers,
  count(distinct case when shop_home_visit > 0 then 1 else 0 end) as sh_browsers,
  count(distinct case when grid_visit > 0 then 1 else 0 end) as grid_browsers,  
  count(distinct case when section_visit > 0 then 1 else 0 end) as section_browsers,
from agg
group by all 


--------------------------------------------------------
--TESTING
--------------------------------------------------------
-- TEST 1: check to see if browsers counts actually work
with agg as (
select e.*
  -- platform,
  -- browser_id,
  -- count(case when event_type in ('shop_home') then sequence_number end) as shop_home_visit,
  -- count(case when event_type in ('shop_home_listing_grid_seen') then sequence_number end)  as grid_visit,
  -- count(case when event_type in ('shop_home_listings_section_seen') then sequence_number end)  as section_visit,
from 
  etsy-data-warehouse-prod.weblog.events e  
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date-30
  and v.platform in ('mobile_web','desktop')
  and event_type in ('shop_home', 'shop_home_listing_grid_seen','shop_home_listings_section_seen')
  and browser_id in ('9615A594124E4514AED4C508C5F6') and platform in ('mobile_web')
group by all
order by sequence_number asc
)
/*_date	run_date	visit_id	event_type	sequence_number	url	referrer	ref_tag	page_view	part_count	mobile_template	order_id	user_id	listing_id	listing_ids	is_preliminary	gdpr_p	gdpr_tp	epoch_ms
2025-03-01	1740787200	9615A594124E4514AED4C508C5F6.1740843941305.2	shop_home	12	http://www.etsy.com/shop/MandaKJane?ref=seller-platform-mcnav	https://www.etsy.com/your/shops/me/customer-service-stats?ref=seller-platform-mcnav	seller-platform-mcnav	1	0	0		29688861			0	3	3	1740843994547
2025-03-01	1740787200	9615A594124E4514AED4C508C5F6.1740843941305.2	shop_home_listings_section_seen	34	http://www.etsy.com/shop/MandaKJane?ref=seller-platform-mcnav	https://www.etsy.com/your/shops/me/customer-service-stats?ref=seller-platform-mcnav	seller-platform-mcnav	0	0	0		29688861			0	3	3	1740843995355
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home	45	http://www.etsy.com/shop/MandaKJane?ref=profile_header	https://www.etsy.com/people/AAbsher?ref=hdr_user_menu-profile	profile_header	1	0	0		29688861			0	3	3	1740602413344
2025-03-01	1740787200	9615A594124E4514AED4C508C5F6.1740843941305.2	shop_home	49	http://www.etsy.com/shop/MandaKJane?ref=seller-platform-mcnav	https://www.etsy.com/your/shops/me/dashboard?ref=hdr-mcpa	seller-platform-mcnav	1	0	0		29688861			0	3	3	1740844007009
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home_listings_section_seen	68	http://www.etsy.com/shop/MandaKJane?ref=profile_header	https://www.etsy.com/people/AAbsher?ref=hdr_user_menu-profile	profile_header	0	0	0		29688861			0	3	3	1740602413711
2025-03-01	1740787200	9615A594124E4514AED4C508C5F6.1740843941305.2	shop_home_listings_section_seen	70	http://www.etsy.com/shop/MandaKJane?ref=seller-platform-mcnav	https://www.etsy.com/your/shops/me/dashboard?ref=hdr-mcpa	seller-platform-mcnav	0	0	0		29688861			0	3	3	1740844007386
2025-03-01	1740787200	9615A594124E4514AED4C508C5F6.1740843941305.2	shop_home_listing_grid_seen	72	http://www.etsy.com/shop/MandaKJane?ref=seller-platform-mcnav	https://www.etsy.com/your/shops/me/dashboard?ref=hdr-mcpa	seller-platform-mcnav	0	0	0		29688861			0	3	3	1740844010363
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home_listing_grid_seen	83	http://www.etsy.com/shop/MandaKJane/edit?ref=edit_trust_header		edit_trust_header	0	0	0		29688861			0	3	3	1740602424384
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home	85	http://www.etsy.com/shop/MandaKJane	https://www.etsy.com/shop/MandaKJane/edit?ref=edit_trust_header		1	0	0		29688861			0	3	3	1740602481414
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home_listings_section_seen	104	http://www.etsy.com/shop/MandaKJane	https://www.etsy.com/shop/MandaKJane/edit?ref=edit_trust_header		0	0	0		29688861			0	3	3	1740602481836
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home_listing_grid_seen	106	http://www.etsy.com/shop/MandaKJane	https://www.etsy.com/shop/MandaKJane/edit?ref=edit_trust_header		0	0	0		29688861			0	3	3	1740602482991
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home	107	http://www.etsy.com/shop/MandaKJane	https://www.etsy.com/shop/MandaKJane/edit?ref=edit_trust_header		1	0	0		29688861			0	3	3	1740602484633
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home_listings_section_seen	127	http://www.etsy.com/shop/MandaKJane	https://www.etsy.com/shop/MandaKJane/edit?ref=edit_trust_header		0	0	0		29688861			0	3	3	1740602484970
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home_listing_grid_seen	129	http://www.etsy.com/shop/MandaKJane	https://www.etsy.com/shop/MandaKJane/edit?ref=edit_trust_header		0	0	0		29688861			0	3	3	1740602486563
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home	166	http://www.etsy.com/shop/MandaKJane?ref=dashboard-header	https://www.etsy.com/your/shops/me/dashboard?ref=hdr-mcpa	dashboard-header	1	0	0		29688861			0	3	3	1740602503982
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home_listings_section_seen	186	http://www.etsy.com/shop/MandaKJane?ref=dashboard-header	https://www.etsy.com/your/shops/me/dashboard?ref=hdr-mcpa	dashboard-header	0	0	0		29688861			0	3	3	1740602504447
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home_listing_grid_seen	187	http://www.etsy.com/shop/MandaKJane?ref=dashboard-header	https://www.etsy.com/your/shops/me/dashboard?ref=hdr-mcpa	dashboard-header	0	0	0		29688861			0	3	3	1740602505709
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home	237	etsy://www.etsy.com/shop/MandaKJane			1	0	0		29688861			0	3	3	1740602621223
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home	252	http://www.etsy.com/shop/MandaKJane?ref=seller-platform-mcnav	https://www.etsy.com/your/shops/me/dashboard?ref=hdr-mcpa	seller-platform-mcnav	1	0	0		29688861			0	3	3	1740602632518
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home_listings_section_seen	274	http://www.etsy.com/shop/MandaKJane?ref=seller-platform-mcnav	https://www.etsy.com/your/shops/me/dashboard?ref=hdr-mcpa	seller-platform-mcnav	0	0	0		29688861			0	3	3	1740602633587
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home_listing_grid_seen	286	http://www.etsy.com/shop/MandaKJane/edit?ref=edit_trust_header		edit_trust_header	0	0	0		29688861			0	3	3	1740602644032
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home_listing_grid_seen	300	http://www.etsy.com/shop/MandaKJane/edit?ref=edit_trust_header		edit_trust_header	0	0	0		29688861			0	3	3	1740602724159
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home	323	etsy://www.etsy.com/shop/57809690	etsy://www.etsy.com/screen/people_account		1	0	0		29688861			0	3	3	1740602887648
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home	541	etsy://www.etsy.com/shop/57809690	etsy://www.etsy.com/screen/people_account		1	0	0		29688861			0	3	3	1740603167511
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home	547	etsy://www.etsy.com/shop/57809690	etsy://www.etsy.com/screen/people_account		1	0	0		29688861			0	3	3	1740603182092
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home	556	etsy://www.etsy.com/shop/57809690	etsy://www.etsy.com/screen/people_account		1	0	0		29688861			0	3	3	1740603191694
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home	558	etsy://www.etsy.com/shop/57809690	etsy://www.etsy.com/screen/people_account		1	0	0		29688861			0	3	3	1740603194452
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home	571	etsy://www.etsy.com/shop/MandaKJane			1	0	0		29688861			0	3	3	1740603197554
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home	672	etsy://www.etsy.com/shop/57809690	etsy://www.etsy.com/screen/people_account		1	0	0		29688861			0	3	3	1740603271844
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home	673				1	0	0		29688861			0	3	3	1740603274234
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home	717	etsy://www.etsy.com/shop/24603772	etsy://www.etsy.com/screen/yr_purchases		1	0	0		29688861			0	3	3	1740603326334
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home	721	etsy://www.etsy.com/shop/25013598	etsy://www.etsy.com/screen/yr_purchases		1	0	0		29688861			0	3	3	1740603336846
2025-02-26	1740528000	9615A594124E4514AED4C508C5F6.1740602405278.2	shop_home	724	etsy://www.etsy.com/shop/25013598	etsy://www.etsy.com/screen/yr_purchases		1	0	0		29688861			0	3	3	1740603339183
*/

-- select platform, browser_id, count(*) from agg group by all order by 3 desc limit 5
-- platform	browser_id	shop_home_visit	grid_visit	section_visit
-- desktop	9615A594124E4514AED4C508C5F6	3	1	1
-- mobile_web	9615A594124E4514AED4C508C5F6	19	7	7
