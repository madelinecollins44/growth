
with favorited_listings as (
select 
  mapped_user_id,
  listing_id,
  max(update_date) as update_date
from 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile
inner join 
  etsy-data-warehouse-prod.etsy_shard.users_favoritelistings using (user_id)
where 
  is_displayable = 1
group by all
)
