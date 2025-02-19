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
		date(b._partitiontime) >= current_date-30
    and v._date >= current_date-30
	  and platform in ('mobile_web','desktop')
    and (beacon.event_name in ('shop_home'))
group by all
)
, mapped_user_visits as ( -- add in mapped user id here so can join to favorites table 
select
  mapped_user_id,
  shop_id,
  seller_user_id,
  visit_id, 
  _date,
  views
from 
  visited_shop_id
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile using (user_id)
)
select
  case when 
from 
  mapped_user_visits v
left join 
  favorited_listings f
    on v.mapped_user_id=f.mapped_user_id
    and v.shop-id=f.shop_id
    and v.visit_date >= f.favoriting_date -- visit has to be before the user favorited a listing from that shop 
