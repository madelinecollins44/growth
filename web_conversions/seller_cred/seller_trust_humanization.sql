/*
begin 
create or replace temp table shop_traffic as (
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
);

create or replace temp table members as (
select distinct 
    shop_id 
  from 
    etsy-data-warehouse-prod.etsy_shard.shop_about_members 
  where 
    is_deleted = 0 
    and role is not null
);

create or replace temp table images as (
select distinct
    shop_id
from 
  etsy-data-warehouse-prod.etsy_shard.shop_about_images
);

create or replace temp table video as (
select distinct 
    shop_id
  from 
    etsy-data-warehouse-prod.etsy_shard.shop_about_videos
  where 
    state=0
);

create or replace temp table links as (
select 
    shop_id,
    max(case when related_links = '[]' or related_links is null then 0 else 1 end) as has_link 
from 
  etsy-data-warehouse-prod.etsy_shard.shop_about
where 
  status in ('active')
group by all 
);
end
*/

select
  active_seller_status,
  case when m.shop_id is not null then 1 else 0 end as has_members,
  case when i.shop_id is not null then 1 else 0 end as has_images,
  case when v.shop_id is not null then 1 else 0 end as has_videos,
  case when l.shop_id =1 then 1 else 0 end as has_link,
  count(distinct b.shop_id) as shops,
  sum(views) as shop_home_traffic,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  etsy-bigquery-adhoc-prod._script8d9c96dd0e81e141d44b4a6997d9f39aec730a85.shop_traffic t
    on cast(b.shop_id as string)=t.shop_id
left join 
  etsy-bigquery-adhoc-prod._scriptdd2e597ab01fcb89dc06725853ebbe1efb1f9af2.members m -- shop members 
      on b.shop_id=m.shop_id
left join 
  etsy-bigquery-adhoc-prod._scriptdd2e597ab01fcb89dc06725853ebbe1efb1f9af2.images i
      on b.shop_id=m.shop_id
left join 
  etsy-bigquery-adhoc-prod._scriptdd2e597ab01fcb89dc06725853ebbe1efb1f9af2.video v
     on b.shop_id=m.shop_id
left join 
  etsy-bigquery-adhoc-prod._scriptdd2e597ab01fcb89dc06725853ebbe1efb1f9af2.links l -- related links 
    on b.shop_id=l.shop_id
group by all


select
  active_seller_status,
  count(distinct b.shop_id) as shops,
  count(distinct case when m.shop_id is not null then b.shop_id end) as has_members,
  count(distinct case when i.shop_id is not null then b.shop_id end) as has_images,
  count(distinct case when v.shop_id is not null then b.shop_id end) as has_videos,
  count(distinct case when l.shop_id =1 then b.shop_id end) as has_link,
  sum(views) as shop_home_traffic,
from 
  etsy-data-warehouse-prod.rollups.seller_basics b
left join 
  etsy-bigquery-adhoc-prod._script8d9c96dd0e81e141d44b4a6997d9f39aec730a85.shop_traffic t
    on cast(b.shop_id as string)=t.shop_id
left join 
  etsy-bigquery-adhoc-prod._scriptdd2e597ab01fcb89dc06725853ebbe1efb1f9af2.members m -- shop members 
      on b.shop_id=m.shop_id
left join 
  etsy-bigquery-adhoc-prod._scriptdd2e597ab01fcb89dc06725853ebbe1efb1f9af2.images i
      on b.shop_id=m.shop_id
left join 
  etsy-bigquery-adhoc-prod._scriptdd2e597ab01fcb89dc06725853ebbe1efb1f9af2.video v
     on b.shop_id=m.shop_id
left join 
  etsy-bigquery-adhoc-prod._scriptdd2e597ab01fcb89dc06725853ebbe1efb1f9af2.links l -- related links 
    on b.shop_id=l.shop_id
group by all


