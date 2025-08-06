---------------------------------------------------------------
-- test 1: make sure listing view count is correct
---------------------------------------------------------------
with first_atc as (
select
  visit_id,
  -- split(visit_id, ".")[0] as browser_id, 
  min(sequence_number) as sequence_number
from 
  etsy-data-warehouse-prod.analytics.listing_views 
where 
  _date >= current_date-30
  and platform in ('boe','mobile_web','desktop')
  and added_to_cart = 1
group by all 
)
, visit_level as (
select
  platform,
  visit_id, 
  case when sequence_number >= f.sequence_number then 1 else 0 end as after_atc,
  count(sequence_number) as listing_views,
  count(distinct listing_id) as listings
from 
  etsy-data-warehouse-prod.analytics.listing_views lv
left join 
  first_atc f
    using (visit_id, sequence_number)
where 
  _date >= current_date-30
  and platform in ('boe','mobile_web','desktop')
group by all 
)
select 
    distinct visit_id
from 
  visit_level
where after_atc =0 -- only look at everything before atc
and listing_views = 1
group by all 
limit 5
  /* 
  visit_id
MWiT6dBPS-VZYyoRWCnQqPj0bGc7.1753485877698.1
W9PFVAn8_NrDYpMWw3nrau7ajH7e.1752877160496.1
SbOuWoLM_ZQJWkYphfvBQcEDuZuD.1753640320236.1
kzq3CwRpgPEwajjMWTs0QtG6nVkv.1753599943450.1
hyJ4G4Snz8Zi2Hr-d5m3c5JAXg2j.1752068224730.1
  */

select * 
from 
  etsy-data-warehouse-prod.analytics.listing_views 
where 
  _date >= current_date-30
  and platform in ('boe','mobile_web','desktop') 
  and visit_id in ('MWiT6dBPS-VZYyoRWCnQqPj0bGc7.1753485877698.1',
'W9PFVAn8_NrDYpMWw3nrau7ajH7e.1752877160496.1',
'SbOuWoLM_ZQJWkYphfvBQcEDuZuD.1753640320236.1',
'kzq3CwRpgPEwajjMWTs0QtG6nVkv.1753599943450.1',
'hyJ4G4Snz8Zi2Hr-d5m3c5JAXg2j.1752068224730.1')
/*
_date	run_date	visit_id	listing_id	platform	sequence_number	is_first_page	event_referrer_type	referring_page_event	ref_tag	referring_page_event_sequence_number	seller_user_id	seller_language	shop_country	detected_region	requested_language	translation_type	epoch_ms	dwell_ms	added_to_cart	favorited	purchased_in_visit	purchased_after_view	price_usd	shipping_price_usd	nudges_seen	partition_key	images_seen	image_count	text_reviews_seen	shop_rating_count	listing_rating_count	buyer_price	buyer_price_currency	edd_shown	passes_pretty_pricing_eligibility_checks	is_bestseller	sale_type	edd_shown_min	edd_shown_max	medd_framework_min_edd	medd_framework_max_edd	click_to_translate	machine_translation_system_id	etsy_transit_times_metadata	display_price_usd	how_its_made_label
2025-07-27	1753574400	kzq3CwRpgPEwajjMWTs0QtG6nVkv.1753599943450.1	1811844590	mobile_web	3	1	Google				248291058	en-US	US	US	en-US	CT	1753599943594		0	0	0	0				1753574400	1	3	3	6	0	2.99	USD		false	false						0			2.99	seller_designed
2025-07-25	1753401600	MWiT6dBPS-VZYyoRWCnQqPj0bGc7.1753485877698.1	825072049	desktop	0	1	Internal	view_listing			283590559	en-US	CN	ES	es	MT	1753485877698		0	0	0	0	23.9	5.44		1753401600	1	1	4	2496	6	24.7	EUR		false	false		2025-08-04	2025-08-15	meets	egregious	0	13	"{""override"":0,""run_ts"":""2025-07-25""}"	22.93	seller_curated
2025-07-18	1752796800	W9PFVAn8_NrDYpMWw3nrau7ajH7e.1752877160496.1	1813019633	mobile_web	3	1	Dark				974805695	en-US	TR	DE	de	MT	1752877160758		0	0	0	0	263.58	0		1752796800	1	9	3	58	1	236.93	EUR	24. Juli-07. Aug.	false	false	percent_discount_on_entire_order	2025-07-24	2025-08-07	exceeds	does_not_meet	0	15	"{""override"":0,""run_ts"":""2025-07-18""}"	158.15	made_by
2025-07-27	1753574400	SbOuWoLM_ZQJWkYphfvBQcEDuZuD.1753640320236.1	1810205938	desktop	0	1	Internal	view_listing			993469044	en-US	US	FR	fr	MT	1753640320236		0	0	0	0	16.33	19.99		1753574400	1	10	4	468	0	17.45	EUR		false	false	percent_discount_on_entire_order	2025-08-04	2025-08-16	exceeds	egregious	0	16	"{""override"":0,""run_ts"":""2025-07-27""}"	9.8	made_by
2025-07-09	1752019200	hyJ4G4Snz8Zi2Hr-d5m3c5JAXg2j.1752068224730.1	4332433688	desktop	31	0	Internal	shop_home	shop_home_active_1	0	1109667022	en-US	US	US	en-US	CT	1752068377746		0	0	0	0	41.75	53.39		1752019200	1	6	0	0	0	41.75	USD		false	false		2025-07-25	2025-08-26			0		[]	41.75	made_by
*/
