----------------------------------------------------------------------
-- OVERALL COUNTS TO CONFIRM
----------------------------------------------------------------------
select
  count(sequence_number) as listing_views,
  sum(purchased_after_view) as purchased_after_view
from 
  etsy-data-warehouse-prod.analytics.listing_views lv
where 1=1
  and referring_page_event in ('shop_home')
  and platform in ('mobile_web','desktop')
  and _date >= current_date-30
group by all 
order by 2 desc 

-- total gms 
with sh_listing_views as (
select
  visit_id,
  listing_id,
  count(sequence_number) as listing_views,
  sum(purchased_after_view) as purchased_after_view
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 1=1
  and referring_page_event in ('shop_home')
  and platform in ('mobile_web','desktop')
  and _date >= current_date-30
group by all 
order by 2 desc 
)
, all_trans as ( -- get all transactions on visit + listing data 
select
	tv.visit_id, 
	t.listing_id, 
  sum(tg.trans_gms_net) as total_gms, 
	count(distinct t.transaction_id) as transactions
from
	`etsy-data-warehouse-prod`.transaction_mart.transactions_visits tv
join
	`etsy-data-warehouse-prod`.transaction_mart.transactions_gms_by_trans tg using(transaction_id)
join
	`etsy-data-warehouse-prod`.transaction_mart.all_transactions t
    on tv.transaction_id = t.transaction_id
where
	tv.date >= current_date-30
group by all
order by 3 desc
)
select
  sum(total_gms) as total_gms
from
  sh_listing_views lv -- only looking at visits that viewed the listing from shop home 
left join 
  all_trans t using (listing_id, visit_id)
where 1=1
group by all

-- total sections
select
count(distinct section_id)
from 
  etsy-data-warehouse-dev.madelinecollins.section_names sn
-- inner join 
--     etsy-data-warehouse-dev.madelinecollins.shop_home_visits  vi
--   on cast(sn.shop_id as string)=vi.shop_id

----------------------------------------------------------------------
-- RUN QUERY TO GET SHOP HOME VISIT INFO 
----------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.shop_home_visits as (
select
  platform,
  beacon.event_name, 
  (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id, 
  (select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id, 
  visit_id, 
  sequence_number,
from
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
inner join 
  etsy-data-warehouse-prod.weblog.visits using (visit_id)
where
  date(_partitiontime) >= current_date-30
  and _date >= current_date-30
  and platform in ('mobile_web','desktop')
  and (beacon.event_name in ('shop_home'))
group by all
);

----------------------------------------------------------------------------------------------------
-- CREATE TABLE TO GET SECTIONS FOR ALL SHOPS (only looking at shops with sections) 
----------------------------------------------------------------------------------------------------
create or replace table etsy-data-warehouse-dev.madelinecollins.section_names as (
with translated_sections as ( -- grab english translations, or whatever translation is set to 1
select 
  *
from etsy-data-warehouse-prod.etsy_shard.shop_sections_translations
qualify row_number() over (
    partition by id 
    order by
        case when language = 5 then 1 else 2 end,  -- Prioritize language = 5
        language asc  -- If no language = 5, take the lowest language number
) = 1
)
select 
  b.shop_id,
  b.user_id as seller_user_id,
  shop_name,
  coalesce(is_etsy_plus,0) as is_etsy_plus,
  seller_tier_new,
  case when (s.shop_id is not null or t.shop_id is not null) then 1 else 0 end as has_sections,
  coalesce(coalesce(nullif(s.name, ''),t.name), 'missing section name') as section_name,
  active_listing_count as active_listings ,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
inner join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id) -- only looking at shops with sections 
left join 
  translated_sections t 
    on s.shop_id=t.shop_id
    and s.id=t.id
where
  active_seller_status = 1 -- active sellers
  and is_frozen = 0  -- not frozen accounts 
  and active_listings > 0 -- shops with active listings
group by all
);
  
---------------------------------------------------------------------------------------------------------------
-- NAME TYPE BY # OF SECTIONS (this looks at section coverage only, does not include gms/ conversion)
---------------------------------------------------------------------------------------------------------------
with visit_info as (
select  
  shop_id, 
  count(distinct visit_id) as unique_visits,
  count(sequence_number) as pageviews
from 
  etsy-data-warehouse-dev.madelinecollins.shop_home_visits 
group by all
)
select  
  -- case 
  --   when regexp_contains(section_name, r'(?i)(sale|discount|cheap|affordable|budget|expensive|premium|luxury|deal|bargain|clearance|off|% off|under\\s?[\\$€£]\\d+|over\\s?[\\$€£]\\d+|[\\$€£]\\d+)\w*')
  --   then 1 else 0 
  -- end as price_section,

  -- case
  --   when regexp_contains(section_name, r'(?i)(mother|mom|father|dad|parent|grandma|grandmother|grandpa|grandfather|wife|husband|boyfriend|girlfriend|partner|bride|groom|couple|friend|best\s?friend|teacher|coach|boss|coworker|colleague|neighbor|baby|infant|newborn|kid|child|children|teen|boy|girl|son|daughter|family|pet|dog|cat)\w*')
  --   then 1 else 0 
  -- end as recipient_section,

  -- case
  --   when regexp_contains(section_name, r'(?i)(birthday|anniversary|wedding|engagement|baby\s?shower|bridal\s?shower|graduation|retirement|housewarming|promotion|new\s?job|new\s?home|farewell|get\s?well|sympathy|thank\s?you|congratulations|valentine|galentine|easter|mother\'?s\s?day|father\'?s\s?day|christmas|xmas|hanukkah|kwanzaa|new\s?year|thanksgiving|halloween|st\s?patrick\'?s\s?day|4th\s?of\s?july|independence\s?day|holiday|ramadan|eid|diwali|hanukkah|graduation|back\s?to\s?school)\w*')
  --   then 1 else 0 
  -- end as occasion_section,

    -- case
    -- when regexp_contains(section_name, r'(?i)(earring|accessor|keychain|ornament|digital|download|pottery|card|decor|pin|magnet|apparel|gift|comic|book|item|top|flower|ring|bracelet|watch|necklace|tumbler|lighting|set|bundle|journal|calendar|drinkware|cup|patch|pendant|charm|brooch|anklet|jewel|clothing|shirt|t-shirt|sweater|hoodie|jacket|coat|dress|skirt|pant|jean|short|shoe|sneaker|boot|heel|sandal|bag|handbag|backpack|wallet|coaster|belt|hat|scarf|glove|sock|home\s?decor|furniture|candle|vase|mug|glass|plate|bowl|cutlery|bedding|pillow|blanket|rug|towel|lamp|mirror|clock|art|painting|poster|print|sticker|toy|game|puzzle|book|notebook|planner|pen|stationery|craft|yarn|fabric|tool|gadget|tech|phone\s?case|charger|headphones|earbuds|laptop\s?stand|tablet\s?case|kitchen|cookware|bakeware|appliance|utensil|pet\s?toy|pet\s?bed|collar|leash|harness|figurine|statue|doll|button|chain)\w*')
    then 1 else 0 
  end as item_section,
  count(section_name) as total_sections,
  count(case when vi.shop_id is not null then section_name end) as visited_shop_sections,
  sum(pageviews) as pageviews
from 
  etsy-data-warehouse-dev.madelinecollins.section_names sn
inner join 
  visit_info vi
    on cast(sn.shop_id as string)=vi.shop_id
group by all
  
---------------------------------------------------------------------------------------------------------------
-- GMS / LISTING VIEWS BY SECTION NAME 
---------------------------------------------------------------------------------------------------------------
with sh_listing_views as (
select
  visit_id,
  listing_id,
  count(sequence_number) as listing_views,
  sum(purchased_after_view) as purchased_after_view
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 1=1
  and referring_page_event in ('shop_home')
  and platform in ('mobile_web','desktop')
  and _date >= current_date-30
group by all 
order by 2 desc 
)
-- 162912016 lv,3285529 pav
, all_trans as ( -- get all transactions on visit + listing data 
select
	tv.visit_id, 
	t.listing_id, 
  sum(tg.trans_gms_net) as total_gms, 
	count(distinct t.transaction_id) as transactions
from
	`etsy-data-warehouse-prod`.transaction_mart.transactions_visits tv
join
	`etsy-data-warehouse-prod`.transaction_mart.transactions_gms_by_trans tg using(transaction_id)
join
	`etsy-data-warehouse-prod`.transaction_mart.all_transactions t
    on tv.transaction_id = t.transaction_id
where
	tv.date >= current_date-30
group by all
order by 3 desc
)
, listing_metrics as ( -- get listing gms from visits that viewed listing from shop home 
select
  lv.listing_id,
  count(distinct lv.visit_id) as unique_visits,
  sum(listing_views) as listing_views,
  sum(purchased_after_view) as purchased_after_view,
  count(distinct t.visit_id) as unique_converting_visits, 
  sum(total_gms) as total_gms
from
  sh_listing_views lv -- only looking at visits that viewed the listing from shop home 
left join 
  all_trans t using (listing_id, visit_id)
where 1=1
group by all
order by 3 desc
)
-- 162912016 lv, 3285529 pav
, section_info as (
select
  -- s.section_id,
  coalesce(section_name,'no section name') as section_name,
	-- shop_name,
  listing_id
from 
  etsy-data-warehouse-dev.madelinecollins.section_names s
left join 
  etsy-data-warehouse-prod.etsy_shard.listings l using (section_id)
where 
	is_displayable > 0 
	and is_available > 0 
group by all
) 
select
  --   case 
  --   when regexp_contains(section_name, r'(?i)(sale|discount|cheap|affordable|budget|expensive|premium|luxury|deal|bargain|clearance|off|% off|under\\s?[\\$€£]\\d+|over\\s?[\\$€£]\\d+|[\\$€£]\\d+)\w*')
  --   then 1 else 0 
  -- end as price_section,

  -- case
  --   when regexp_contains(section_name, r'(?i)(mother|mom|father|dad|parent|grandma|grandmother|grandpa|grandfather|wife|husband|boyfriend|girlfriend|partner|bride|groom|couple|friend|best\s?friend|teacher|coach|boss|coworker|colleague|neighbor|baby|infant|newborn|kid|child|children|teen|boy|girl|son|daughter|family|pet|dog|cat)\w*')
  --   then 1 else 0 
  -- end as recipient_section,

  case
    when regexp_contains(section_name, r'(?i)(birthday|anniversary|wedding|engagement|baby\s?shower|bridal\s?shower|graduation|retirement|housewarming|promotion|new\s?job|new\s?home|farewell|get\s?well|sympathy|thank\s?you|congratulations|valentine|galentine|easter|mother\'?s\s?day|father\'?s\s?day|christmas|xmas|hanukkah|kwanzaa|new\s?year|thanksgiving|halloween|st\s?patrick\'?s\s?day|4th\s?of\s?july|independence\s?day|holiday|ramadan|eid|diwali|hanukkah|graduation|back\s?to\s?school)\w*')
    then 1 else 0 
  end as occasion_section,

  --   case
  --   when regexp_contains(section_name, r'(?i)(earring|accessor|keychain|ornament|digital|download|pottery|card|decor|pin|magnet|apparel|gift|comic|book|item|top|flower|ring|bracelet|watch|necklace|tumbler|lighting|set|bundle|journal|calendar|drinkware|cup|patch|pendant|charm|brooch|anklet|jewel|clothing|shirt|t-shirt|sweater|hoodie|jacket|coat|dress|skirt|pant|jean|short|shoe|sneaker|boot|heel|sandal|bag|handbag|backpack|wallet|coaster|belt|hat|scarf|glove|sock|home\s?decor|furniture|candle|vase|mug|glass|plate|bowl|cutlery|bedding|pillow|blanket|rug|towel|lamp|mirror|clock|art|painting|poster|print|sticker|toy|game|puzzle|book|notebook|planner|pen|stationery|craft|yarn|fabric|tool|gadget|tech|phone\s?case|charger|headphones|earbuds|laptop\s?stand|tablet\s?case|kitchen|cookware|bakeware|appliance|utensil|pet\s?toy|pet\s?bed|collar|leash|harness|figurine|statue|doll|button|chain)\w*')
  --   then 1 else 0 
  -- end as item_section,
  sum(listing_views) as listing_views,
  sum(purchased_after_view) as purchased_after_view,
  sum(total_gms) as total_gms
from listing_metrics
left join section_info using (listing_id)
group by all 
order by occasion_section asc

--------------------------------------------------
-- share of sections without names 
--------------------------------------------------
with translated_sections as ( -- grab english translations, or whatever translation is set to 1
select 
  *
from etsy-data-warehouse-prod.etsy_shard.shop_sections_translations
qualify row_number() over (
    partition by id 
    order by
        case when language = 5 then 1 else 2 end,  -- Prioritize language = 5
        language asc  -- If no language = 5, take the lowest language number
) = 1
)
select 
  count(s.id) as names,
  sum(case when coalesce(nullif(s.name, ''),t.name) is not null then 1 else 0 end) as filled_in,
  sum(case when coalesce(nullif(s.name, ''),t.name) is null then 1 else 0 end) as missing_ids,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
inner join 
  etsy-data-warehouse-prod.etsy_shard.shop_sections s using (shop_id)
left join 
  translated_sections t 
    on s.shop_id=t.shop_id
    and s.id=t.id
where
  active_seller_status = 1 -- active sellers
  and is_frozen = 0  -- not frozen accounts 
  and active_listings > 0 -- shops with active listings
group by all
-- names	filled_in	missing_ids
-- 9585659	9568522	17137

--------------------------------------------------
-- TESTING
--------------------------------------------------
-- TEST 1: how many section names are getting repeated? is this because they actually show up twice? 
select shop_id, shop_name, section_name, count(*) from etsy-data-warehouse-dev.madelinecollins.section_names where section_name not in ('missing section name') and active_listings > 0 group by all order by 4 desc limit 5
-- shop_id	shop_name	section_name	f0_
-- 13042804	SoleilDuNordShop		5
-- 5437285	ahldraws	prints	3 --------> SHOWS UP THREE TIMES 
-- 10972477	Maryandpatch	Patterns in French	2--------> SHOWS UP TWICE  
-- 38539730	HelloEloCha	Spring	2--------> SHOWS UP TWICE  
-- 33053189	ChromaticsDesigns	Art	2--------> SHOWS UP TWICE  

with agg as (
select 
  shop_id, 
  shop_name, 
  section_name, 
  count(*) as total_count
from etsy-data-warehouse-dev.madelinecollins.section_names 
where 
  section_name not in ('missing section name')
  and active_listings > 0 
group by all 
order by 4 desc 
)
select 
  total_count,
  count(section_name) as names
from agg 
group by all
order by 1 asc
-- total_count	names
-- 1	6498023
-- 2	112
-- 3	1
-- 5	1

-- TEST 2: make sure listing metrics make sense 
with sh_listing_views as (
select
  visit_id,
  listing_id,
  count(sequence_number) as listing_views,
  sum(purchased_after_view) as purchased_after_view
from 
  etsy-data-warehouse-prod.analytics.listing_views
where 1=1
  and referring_page_event in ('shop_home')
  and platform in ('mobile_web','desktop')
  and _date >= current_date-30
group by all 
order by 2 desc 
)
-- , listing_gms as ( -- get listing gms from visits that viewed listing from shop home 
select
  -- listing_id,
  count(distinct lv.visit_id) as visits, -- this should match # of purchases after view from above cte?
  sum(gms_net) as gms_net
from 
  (select distinct visit_id from sh_listing_views) lv -- only looking at visits that viewed the listing from shop home 
inner join 
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv using (visit_id)
inner join 
  etsy-data-warehouse-prod.transaction_mart.all_transactions a
    on tv.transaction_id=a.transaction_id
inner join 
  etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans t 
    on t.transaction_id=a.transaction_id
where 1=1
  and a.date >= current_date-365 -- gms over last year 
  and listing_id in (1873482987)
group by all
-- )
-- select
--   lv.listing_id,
--   sum(listing_views) as sh_listing_views,
--   sum(purchased_after_view) as purchased_after_view, 
--   sum(gms_net) as sh_gms_net
-- from  
--   sh_listing_views lv
-- left join 
--   listing_gms g using (listing_id, visit_id)
-- group by all 
-- order by 2 desc
-- limit 50
