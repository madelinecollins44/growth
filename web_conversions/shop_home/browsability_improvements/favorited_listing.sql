------------------------------------------------------------------------------------------
-- what % of visits view a shop home page of a listing they have favorited
------------------------------------------------------------------------------------------
with favorited_listings as ( -- listings each user had favorited at specific time 
select 
  mapped_user_id,
  shop_id,
  shop_user_id as seller_user_id,
  date(timestamp_seconds(create_date)) as favoriting_date,
  count(distinct listing_id) as listings
from 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile
inner join 
  etsy-data-warehouse-prod.etsy_shard.users_favoritelistings using (user_id)
where 
  is_displayable = 1
group by all
)
, visited_shop_id as ( -- get visit info for each user to shop home page 
select
  beacon.event_name,
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
group by all
)
, mapped_user_visits as ( -- add in mapped user id here so can join to favorites table 
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
)
, favorites_with_flag AS ( -- at the time of the visit, did the user have a listing favorited from the shop? 
select
  f.mapped_user_id,
  f.shop_id,
  v.visit_date,
  count(f.shop_id) AS total_favorites,
  case
    when count(f.shop_id) > 0 then 1
    else 0
  end as had_favorite_at_visit
from favorited_listings f
join mapped_user_visits v
  on f.mapped_user_id = v.mapped_user_id
  and cast(f.shop_id as string)= v.shop_id
  and f.favoriting_date <= v.visit_date
group by all
)
select
  coalesce(fwf.had_favorite_at_visit, 0) AS had_favorite_at_visit,
  count(v.visits) as visits,
  sum(v.visits) as visits_sum,
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
