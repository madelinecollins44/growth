with shop_traffic as (
select
  (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id,
  count(sequence_number) as views
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`b
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
	where 1=1
  and v._date >= current_date-30
  and v.platform in ('mobile_web','desktop')
	and date(_partitiontime) >= current_date-30
  and beacon.event_name in ('shop_home')
group by all
)
select
  active_seller_status,
  count(distinct b.shop_id) as shops,
  sum(views) as shop_home_traffic,

from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  shop_traffic t
    on cast(b.shop_id as string)=t.shop_id
left join 
  (select distinct 
    shop_id 
  from 
    etsy-data-warehouse-prod.etsy_shard.shop_about_members 
  where 
    is_deleted = 0 
    and role is not null) m -- shop members 
      on b.shop_id=m.shop_id
left join 
  (select distinct
    shop_id
  from 
    etsy-data-warehouse-prod.etsy_shard.shop_about_images) i
      on b.shop_id=m.shop_id
left join 
  (select distinct 
    shop_id
  from 
    etsy-data-warehouse-prod.etsy_shard.shop_about_videos
  where 
    state=0) v
     on b.shop_id=m.shop_id
left join 
  (select 
    shop_id,
    case when related_links != '[]' then 0 else 1 end as has_link 
  from 
    etsy-data-warehouse-prod.etsy_shard.shop_about) l -- related links 
    on b.shop_id=l.shop_id

