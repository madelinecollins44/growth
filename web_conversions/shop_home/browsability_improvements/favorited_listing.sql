------------------------------------------------------------------------------------------
-- what % of visits view a shop home page of a listing they have favorited
------------------------------------------------------------------------------------------
with favorited_listings as (
select 
  mapped_user_id,
  listing_id,
  shop_id,
  date(timestamp_seconds(create_date)) as create_date,
  -- max(date(timestamp_seconds(update_date))) as most_recent_update,
from 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile
inner join 
  etsy-data-warehouse-prod.etsy_shard.users_favoritelistings using (user_id)
where 
  is_displayable = 1
group by all
order by 4 desc
)
, visited_shop_id as (
select
  beacon.event_name,
  user_id,
  (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id,
  visit_id, 
  date(b._partitiontime) as visit_id,
  count(visit_id) as views
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
