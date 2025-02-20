------------------------------------------------------------------------------------------
-- of visits that come to shop home with favorites from that shop, how many favorites do they have? 
------------------------------------------------------------------------------------------
with favorited_listings as ( -- listings each user had favorited at specific time 
select 
  mapped_user_id,
  shop_id,
  shop_user_id as seller_user_id,
  date(timestamp_seconds(create_date)) as favoriting_date,
  listing_id
from 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile
inner join 
  etsy-data-warehouse-prod.etsy_shard.users_favoritelistings using (user_id)
where 
  is_displayable = 1
  and shop_id > 0 
group by all
)
, visited_shop_id as ( -- get visit info for each user to shop home page 
select
  beacon.event_name,
  date(b._partitiontime) as visit_date,
  user_id,
  (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id,
  (select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id,
  visit_id, 
  count(visit_id) as pageviews
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` b
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
	where
		date(b._partitiontime) >= current_date-30
    and v._date >= current_date-30
	  and platform in ('mobile_web','desktop')
    and (beacon.event_name in ('shop_home'))
    and user_id is not null 
group by all
)
, mapped_user_visits as ( -- add in mapped user id here so can join to favorites table 
select
  mapped_user_id,
  shop_id,
  seller_user_id,
  visit_id, 
  visit_date,
  pageviews
from 
  visited_shop_id
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile using (user_id)
group by all 
)
, favorited_counts as (
  select
  v.mapped_user_id,
  v.shop_id,
  v.visit_date,
  v.visit_id,
  count(distinct f.listing_id) AS num_favorited_listings -- how many listings were liked before time of visit
from 
  mapped_user_visits v
left join 
  favorited_listings f
  on v.mapped_user_id = f.mapped_user_id
  and cast(f.shop_id as string) = v.shop_id
  and f.favoriting_date < v.visit_date -- favorite was before the visit
group by all
)
select
  num_favorited_listings,
  count(distinct visit_id) as visits 
from 
  favorited_counts
group by all

	
------------------------------------------------------------------------------------------
-- what % of visits view a shop home page of a listing they have favorited
------------------------------------------------------------------------------------------
with favorited_listings as ( -- listings each user had favorited at specific time 
select 
  mapped_user_id,
  shop_id,
  shop_user_id as seller_user_id,
  min(date(timestamp_seconds(create_date))) as favoriting_date, -- oldest date that user favorited something from this shop
  count(distinct listing_id) as listings
from 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile
inner join 
  etsy-data-warehouse-prod.etsy_shard.users_favoritelistings using (user_id)
where 
  is_displayable = 1
  and shop_id > 0 
group by all
)
, visited_shop_id as ( -- get visit info for each user to shop home page 
select
  beacon.event_name,
  date(b._partitiontime) as visit_date,
  user_id,
  (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id,
  (select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id,
  visit_id, 
  count(visit_id) as pageviews
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` b
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
	where
		date(b._partitiontime) >= current_date-30
    and v._date >= current_date-30
	  and platform in ('mobile_web','desktop')
    and (beacon.event_name in ('shop_home'))
    and user_id is not null 
group by all
)
, mapped_user_visits as ( -- add in mapped user id here so can join to favorites table 
select
  mapped_user_id,
  shop_id,
  seller_user_id,
  visit_id, 
  visit_date,
  pageviews
from 
  visited_shop_id
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile using (user_id)
group by all 
)
, favorites_with_flag as ( -- at the time of the visit, did the user have a listing favorited from the shop? 
select
  f.mapped_user_id,
  f.shop_id,
  v.visit_date,
  f.favoriting_date,
  case
    when f.favoriting_date < v.visit_date then 1
    else 0
  end as had_favorite_at_visit,
  count(distinct visit_id) as visits, 
from 
  favorited_listings f
join 
  mapped_user_visits v
    on f.mapped_user_id = v.mapped_user_id
    and cast(f.shop_id as string)= v.shop_id
group by all
)
select
  coalesce(fwf.had_favorite_at_visit, 0) AS had_favorite_at_visit,
  count(distinct v.visit_id) as visits,
  sum(v.pageviews) as pageviews,
from 
  mapped_user_visits v
left join 
  favorites_with_flag fwf
    on v.mapped_user_id = fwf.mapped_user_id
    and cast(fwf.shop_id as string)= v.shop_id
    and v.visit_date = fwf.visit_date
group by all


---------------------------------------------------------------------------------------------------------------------------------------------
--TESTING
---------------------------------------------------------------------------------------------------------------------------------------------
-- TEST 1: look at mapped_user_id + date level
select
  v.mapped_user_id,
  visit_date,
  case when f.mapped_user_id is not null and f.shop_id is not null then 1 else 0 end as had_listing_favorited,
  sum(visits) as total_visits,
  sum(pageviews) as total_pageviews
from 
  mapped_user_visits v
left join 
  favorited_listings f
    on v.mapped_user_id=f.mapped_user_id
    and cast(f.shop_id as string)=v.shop_id
    and v.visit_date >= f.favoriting_date -- visit has to be before the user favorited a listing from that shop 
group by all

-- find visits to shop home by date
	with visited_shop_id as ( -- get visit info for each user to shop home page 
select
  -- beacon.event_name,
  user_id,
  (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id,
  (select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id,
  count(distinct visit_id) as visits, 
  date(b._partitiontime) as visit_date,
  count(visit_id) as pageviews
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` b
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where
	date(b._partitiontime) >= current_date-5
  and v._date >= current_date-5
	and platform in ('mobile_web','desktop')
  and (beacon.event_name in ('shop_home'))
  and user_id is not null -- only looking at signed in visits
group by all
)
select
  mapped_user_id,
  shop_id,
  seller_user_id,
  visits, 
  visit_date,
  pageviews
from 
  visited_shop_id
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile using (user_id)
order by 4 desc 
limit 10
-- mapped_user_id	shop_id	seller_user_id	visits	visit_date	pageviews
-- 378480957	25375380	365062654	59	2025-02-17	141
-- 114890745	16955501	114890745	44	2025-02-15	197
-- 694514105	8919300	41671387	39	2025-02-18	167
-- 114890745	16955501	114890745	38	2025-02-16	138
-- 114890745	16955501	114890745	36	2025-02-14	189
-- 378480957	25375380	365062654	35	2025-02-14	106
-- 1025373426	27604842	437378096	35	2025-02-17	4042
-- 114890745	16955501	114890745	34	2025-02-17	128
-- 378480957	25375380	365062654	33	2025-02-18	95
-- 89250615	22481767	89250615	32	2025-02-17	287

-- TEST 2:  understand why visits are getting double counted 
, agg as (
select
  num_favorited_listings,
  visit_id,
  shop_id,
  count(distinct visit_id) as visits 
from 
  favorited_counts
group by all
)
select
  visit_id,
  count(distinct num_favorited_listings)
from agg
group by all 
order by 2 desc 
limit 10

--, agg as (
select
  num_favorited_listings,
  visit_id,
  shop_id,
  count(distinct visit_id) as visits 
from 
  favorited_counts
group by all
)
, AGG_2 as (
select
  visit_id,
  count(distinct num_favorited_listings) as num_favorited_listings
from agg
group by all 
order by 2 desc 
)
select 
  count(distinct case when num_favorited_listings = 0 then visit_id end) as visits_no_likes,
  count(distinct case when num_favorited_listings = 1 then visit_id end) as visits_1_likes,
  count(distinct case when num_favorited_listings > 1 then visit_id end) as visits_more_1_likes,
  count(distinct visit_id) as total_visits
from agg_2

