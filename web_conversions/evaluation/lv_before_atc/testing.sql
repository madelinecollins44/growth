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
select distinct visit_id
  -- case when listing_views <= 100 then cast(listing_views as string) else '101+' end as listing_views,
  -- count(distinct visit_id) as visits,
from 
  visit_level
inner join 
  (select distinct visit_id from visit_level where after_atc = 1) -- looking at visits that did atc 
    using (visit_id)
where 
  after_atc =0 -- only look at everything before atc
  and listing_views = 1
group by all
limit 5
-- order by 1 asc
/* 
visit_id
OH8qCw19SsGJLDmmKCgieur410jj.1753740623276.3
sqFhgKhMjJ8E-2OTBs0uNNkp09II.1751931816378.1
3mq139_WeVAweWOtvhf-uOaQ9fL3.1753373116771.1
cmY80j6Petj8UdN3nWCAXe7a92Tq.1754228700264.2
cstf48cZrNCfFqD837RQb-3Kz0Gm.1752106791570.1
*/

select * 
from 
  etsy-data-warehouse-prod.analytics.listing_views 
where 
  _date >= current_date-30
  and platform in ('boe','mobile_web','desktop') 
  and visit_id in ('OH8qCw19SsGJLDmmKCgieur410jj.1753740623276.3',
'sqFhgKhMjJ8E-2OTBs0uNNkp09II.1751931816378.1',
'3mq139_WeVAweWOtvhf-uOaQ9fL3.1753373116771.1',
'cmY80j6Petj8UdN3nWCAXe7a92Tq.1754228700264.2',
'cstf48cZrNCfFqD837RQb-3Kz0Gm.1752106791570.1')
/* 
_date	run_date	visit_id	listing_id	platform	sequence_number	is_first_page	event_referrer_type	referring_page_event	ref_tag	referring_page_event_sequence_number	seller_user_id	seller_language	shop_country	detected_region	requested_language	translation_type	epoch_ms	dwell_ms	added_to_cart	favorited	purchased_in_visit	purchased_after_view	price_usd	shipping_price_usd	nudges_seen	partition_key	images_seen	image_count	text_reviews_seen	shop_rating_count	listing_rating_count	buyer_price	buyer_price_currency	edd_shown	passes_pretty_pricing_eligibility_checks	is_bestseller	sale_type	edd_shown_min	edd_shown_max	medd_framework_min_edd	medd_framework_max_edd	click_to_translate	machine_translation_system_id	etsy_transit_times_metadata	display_price_usd	how_its_made_label
2025-07-28	1753660800	OH8qCw19SsGJLDmmKCgieur410jj.1753740623276.3	1564081463	mobile_web	113	0	Google				295236098	en-US	UA	GB	en-GB	CT	1753740643539		0	0	0	0	18	0		1753660800	1	10	3	7385	86	16.78	GBP	05-12 Aug	false	true	percent_discount_on_entire_order	2025-08-04	2025-08-11	exceeds	meets	0		[]	9	made_by
2025-07-28	1753660800	OH8qCw19SsGJLDmmKCgieur410jj.1753740623276.3	1564081463	mobile_web	0	1	Google				295236098	en-US	UA	GB	en-GB	CT	1753740623276	16904	1	0	0	0	18	0		1753660800	1	10	3	7385	86	16.78	GBP	05-12 Aug	false	true	percent_discount_on_entire_order	2025-08-04	2025-08-11	exceeds	meets	0		[]	9	made_by
2025-07-08	1751932800	sqFhgKhMjJ8E-2OTBs0uNNkp09II.1751931816378.1	1418770156	mobile_web	188	0	Google				690749260	en-US	US	US	en-US	CT	1751931923423	10345	1	0	1	1	60.4	0		1751932800	1	5	3	4223	7	60.4	USD	Jul 10-15	false	false	percent_discount_on_items_in_set	2025-07-09	2025-07-15	exceeds	meets	0		"{""override"":false,""run_ts"":""2025-07-07""}"	45.3	made_by
2025-07-08	1751932800	sqFhgKhMjJ8E-2OTBs0uNNkp09II.1751931816378.1	1418770156	mobile_web	3	1	Google				690749260	en-US	US	US	en-US	CT	1751931816587	63460	0	0	1	1	60.4	0		1751932800	1	5	3	4223	7	60.4	USD	Jul 10-15	false	false	percent_discount_on_items_in_set	2025-07-09	2025-07-15	exceeds	meets	0		"{""override"":false,""run_ts"":""2025-07-07""}"	45.3	made_by
2025-08-03	1754179200	cmY80j6Petj8UdN3nWCAXe7a92Tq.1754228700264.2	1491379475	mobile_web	0	1	Google				507514914	en-US	TR	AU	en-GB	CT	1754228700264	64896	1	0	0	0	210	200		1754179200	1	6	3	225	10	340.91	AUD	09-20 Aug	false	false	percent_discount_on_items_in_set	2025-08-09	2025-08-20	exceeds	egregious	0		"{""override"":0,""run_ts"":""2025-08-03""}"	105	made_by
2025-08-03	1754179200	cmY80j6Petj8UdN3nWCAXe7a92Tq.1754228700264.2	1491379475	mobile_web	120	0	Internal	cart_view	cart	108	507514914	en-US	TR	AU	en-GB	CT	1754228770996	119403	0	0	0	0	210	200		1754179200	1	6	4	225	10	340.91	AUD	09-20 Aug	false	false	percent_discount_on_items_in_set	2025-08-09	2025-08-20	exceeds	egregious	0		"{""override"":0,""run_ts"":""2025-08-03""}"	105	made_by
2025-07-10	1752105600	cstf48cZrNCfFqD837RQb-3Kz0Gm.1752106791570.1	1688857223	mobile_web	55	0	Google				903444697	en-US	CZ	US	en-US	CT	1752106825719		0	0	0	0				1752105600	1	5	0	0	0	3.04	USD		false	false						0			2.92	seller_designed
2025-07-10	1752105600	cstf48cZrNCfFqD837RQb-3Kz0Gm.1752106791570.1	1688857223	mobile_web	3	1	Google				903444697	en-US	CZ	US	en-US	CT	1752106791691	32476	1	0	0	0				1752105600	1	5	0	0	0	3.04	USD		false	false						0			2.92	seller_designed
2025-07-24	1753315200	3mq139_WeVAweWOtvhf-uOaQ9fL3.1753373116771.1	4338415649	mobile_web	89	0	Internal	search	sr_gallery-1-7	42	814434186	de	DE	DE	de	CT	1753373151083	6822	1	0	0	0	46.5	6.01		1753315200	1	3	4	174	0	37.9	EUR	31. Juli-06. Aug.	false	false		2025-07-31	2025-08-06	meets	egregious	0		"{""override"":false,""ships_on_saturday"":true}"	44.59	seller_designed
2025-07-24	1753315200	3mq139_WeVAweWOtvhf-uOaQ9fL3.1753373116771.1	4338415649	mobile_web	161	0	Internal	search	sr_gallery-1-7	42	814434186	de	DE	DE	de	CT	1753373159294	1250	0	0	0	0	46.5	6.01		1753315200	1	3	4	174	0	37.9	EUR	31. Juli-06. Aug.	false	false		2025-07-31	2025-08-06	meets	egregious	0		"{""override"":false,""ships_on_saturday"":true}"	44.59	seller_designed
*/

----------------------------------------------------------------------------------------------------------------
-- test 2: make sure number of visits w atc matches 
----------------------------------------------------------------------------------------------------------------
select
  count(distinct visit_id) as viists,
  -- split(visit_id, ".")[0] as browser_id, 
  min(sequence_number) as sequence_number
from 
  etsy-data-warehouse-prod.analytics.listing_views 
where 
  _date >= current_date-30
  and platform in ('boe','mobile_web','desktop')
  and added_to_cart = 1
group by all 
/* viists	sequence_number
24272103	0 */

----------------------------------------------------------------------------------------------------------------
-- test 3: make sure first_atc cte works
----------------------------------------------------------------------------------------------------------------
select
  visit_id,
  min(sequence_number) as sequence_number
from 
  etsy-data-warehouse-prod.analytics.listing_views 
where 
  _date >= current_date-30
  and platform in ('boe','mobile_web','desktop')
  and added_to_cart = 1
group by all 
LIMIT 5
/* 
visit_id	sequence_number
316C4BD9D63A487EB629B38E8F58.1752497536144.1	323
3271CD892F7A429B9E6A095EFD20.1752465263413.1	304
52F5DA422E43406199D98049237A.1752456445596.2	1086
5439011A2EC34205AF5EEE64AF56.1752451072026.1	853
54099E598162457DA060E362604E.1752463214071.1	999
*/

select
  visit_id,
  sequence_number
from 
  etsy-data-warehouse-prod.analytics.listing_views 
where 
  _date >= current_date-30
  and platform in ('boe','mobile_web','desktop')
  and added_to_cart = 1
  and visit_id in ('54099E598162457DA060E362604E.1752463214071.1')
group by all 
order by 2 asc
/* 
visit_id	sequence_number
316C4BD9D63A487EB629B38E8F58.1752497536144.1	323
316C4BD9D63A487EB629B38E8F58.1752497536144.1	690

visit_id	sequence_number
3271CD892F7A429B9E6A095EFD20.1752465263413.1	304

visit_id	sequence_number
5439011A2EC34205AF5EEE64AF56.1752451072026.1	853
5439011A2EC34205AF5EEE64AF56.1752451072026.1	1216
5439011A2EC34205AF5EEE64AF56.1752451072026.1	3111
*/

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- test 4: testing to see visit level before_atc + # of lv stats 
------ if someone has 1 LV + before_ATC= 1 and listing_views = 1 then ATC on first listing. if 1+ LV + before_ATC= 1 and listing_views = 1 then ATC on second listing. 
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
, visit_stats as (
select
  platform,
  visit_id, 
  count(sequence_number) as listing_views,
  count(distinct listing_id) as listings,
  sum(added_to_cart) as atcs
from 
  etsy-data-warehouse-prod.analytics.listing_views lv
where 
  _date >= current_date-30
  and platform in ('boe','mobile_web','desktop')
group by all 
)
select
  lv.platform,
  lv.visit_id,
  case when lv.sequence_number <= f.sequence_number then 1 else 0 end as before_first_atc, 
  case when vs.listing_views = 1 then '1 LV' else '1+ LV' end as visit_view_type,
  count(lv.sequence_number) as listing_views,
  count(distinct listing_id) as listings,
  count(distinct lv.visit_id) as visits
from 
  etsy-data-warehouse-prod.analytics.listing_views lv
inner join 
  first_atc f
    on lv.visit_id=f.visit_id
left join 
  visit_stats vs
    on vs.visit_id=lv.visit_id
where 
  _date >= current_date-30
  and lv.platform in ('boe','mobile_web','desktop')
    and lv.visit_id in ('NvLA8ZCAV_PZYl0nVPZam58LIwjL.1754185642538.1')
    -- and listing_views = 1
group by all order by 5 asc
limit 3
/* platform	visit_id	before_first_atc	f0_	listing_views	listings	visits
boe	_u5euiFRSh-YyypsV-2HCw.1754369615780.1	0	1+ LV	4699	5	1
desktop	uoUqElmnzfDo2DlZNv4pXz8Ssfkj.1752216718729.2	1	1+ LV	743	711	1
boe	7A5A951457F54527878C1A1A741E.1753781097996.3	0	1+ LV	736	175	1
mobile_web	HTdrEmU2AvV1rybpCSMN5I_-bXuz.1752325990297.1	1	1+ LV	1	1	1
mobile_web	lL9PTXQe0H_9UhlPmRKYqdoezC7K.1752340736014.1	0	1+ LV	1	1	1
desktop	tMrf2YvTXc2KXj-MQV_O3ERHmp9a.1753055920748.1	1	1+ LV	1	1	1
desktop	NvLA8ZCAV_PZYl0nVPZam58LIwjL.1754185642538.1	1	1 LV	1	1	1
desktop	yQ3GhCLT4jzdI_qvNLASQlmI4LS5.1754337313220.2	1	1 LV	1	1	1
desktop	I3ZQFPkPzoLBl3qmuUS3V68KJBC0.1752328307233.1	1	1 LV	1	1	1
*/

select *
from 
  etsy-data-warehouse-prod.analytics.listing_views 
where 
  _date >= current_date-30
  and visit_id in ('NvLA8ZCAV_PZYl0nVPZam58LIwjL.1754185642538.1')
group by all 
order by 
sequence_number asc
