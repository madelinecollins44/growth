------------------------------------------------------------------------------------------
-- what % of visits view a shop home page of a listing they have favorited
------------------------------------------------------------------------------------------
with favorited_listings as (
select 
  mapped_user_id,
  listing_id,
  shop_id,
  count(distinct create_date),
  -- max(create_date) as create_date
  -- max(update_date) as update_date
from 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile
inner join 
  etsy-data-warehouse-prod.etsy_shard.users_favoritelistings using (user_id)
where 
  is_displayable = 1
group by all
)
select *, count(*) from favorited_listings group by all order by 2 desc 
