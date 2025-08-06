----------------------------------------------------------------------------------------------------------------
-- LISTING VIEWS THAT HAPPEN BEFORE ATC BY PLATFORM AMONG VISITS THAT ATC
----------------------------------------------------------------------------------------------------------------
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
  platform,
  count(lv.sequence_number) as listing_views,
  count(distinct listing_id) as listings,
  count(distinct lv.visit_id) as visits
from 
  etsy-data-warehouse-prod.analytics.listing_views lv
left join 
  first_atc f
    on lv.visit_id=f.visit_id
where 
  _date >= current_date-30
  and platform in ('boe','mobile_web','desktop')
  and lv.sequence_number<= f.sequence_number -- all seq before atc
group by all 
order by 1 desc

------------------------ BY PLATFORM
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
, agg as (
select
  platform,
  lv.visit_id,
  count(lv.sequence_number) as listing_views,
  count(distinct listing_id) as listings,
  count(distinct lv.visit_id) as visits
from 
  etsy-data-warehouse-prod.analytics.listing_views lv
left join 
  first_atc f
    on lv.visit_id=f.visit_id
where 
  _date >= current_date-30
  and platform in ('boe','mobile_web','desktop')
  and lv.sequence_number<= f.sequence_number -- all seq before atc
group by all
)
select 
  platform, 
  count(distinct visit_id) as visits,
  sum(listing_views) as total_lv,
  avg(listing_views) as avg_total_lv,
  -- sum(atcs) as total_atcs,
  -- avg(atcs) as avg_atcs
from agg
group by all 
order by 1 desc


----------------------------------------------------------------------------------------------------------------
-- LISTING VIEWS THAT HAPPEN BEFORE ATC BY PLATFORM AMONG VISITS THAT ATC FOR VISITS W/ 1+ LV
----------------------------------------------------------------------------------------------------------------
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
  count(lv.sequence_number) as listing_views,
  count(distinct listing_id) as listings,
  count(distinct lv.visit_id) as visits
from 
  etsy-data-warehouse-prod.analytics.listing_views lv
left join 
  first_atc f
    on lv.visit_id=f.visit_id
inner join 
  visit_stats vs  
    on vs.visit_id=lv.visit_id
    and listing_views > 1 -- visits w 1+ lv
where 
  _date >= current_date-30
  and lv.platform in ('boe','mobile_web','desktop')
  and lv.sequence_number<= f.sequence_number -- all seq before atc
group by all 
order by 1 desc
  
----------------------------------------------------------------------------------------------------------------
-- AVG LISTING VIEWS AMONG VISITS THAT ATC BY PLATFORM
----------------------------------------------------------------------------------------------------------------
with visits_w_atc as (
select
  distinct visit_id
from 
  etsy-data-warehouse-prod.analytics.listing_views 
where 
  _date >= current_date-30
  and platform in ('boe','mobile_web','desktop')
  and added_to_cart = 1
group by all 
)
, lv_for_visits_w_atc as (
select
  platform,
  visit_id,
  count(sequence_number) as total_lv,
  sum(added_to_cart) as atcs
from 
  etsy-data-warehouse-prod.analytics.listing_views 
inner join 
  visits_w_atc using (visit_id)
where 
  _date >= current_date-30
  and platform in ('boe','mobile_web','desktop')
group by all 
)
select 
  platform,
  count(distinct visit_id) as visits,
  sum(total_lv) as total_lv,
  avg(total_lv) as avg_total_lv,
  sum(atcs) as total_atcs,
  avg(atcs) as avg_atcs
from 
  lv_for_visits_w_atc
group by all 
order by 1 desc
  
----------------------------------------------------------------------------------------------------------------
-- SHARE OF VISITS THAT ATC BY # OF LISTINGS VIEWED
----------------------------------------------------------------------------------------------------------------
with visit_stats as (
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
  platform,
  case when listing_views = 1 and atcs = 1 then '1 LV' else '1+ LV' end,
  count(distinct visit_id) as visits,
  sum(listing_views) as total_lv
from 
  visit_stats
where 
  atcs > 0 -- visits that atc at least once
group by all 
order by 1 desc
  
----------------------------------------------------------------------------------------------------------------
-- NUMBER OF LISTING VIEWS AMONG VISITS THAT ADDED TO CART 
----------------------------------------------------------------------------------------------------------------
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
, lv_by_atc as (
select
  platform,
  visit_id, 
  case when lv.sequence_number >= f.sequence_number then 0 else 1 end as before_atc, -- marks if lv happened before or after first atc
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
  s.platform,
  case when l.listing_views<= 100 then cast(l.listing_views as string) else '101+' end as listing_views,
  count(distinct l.visit_id) as visits,
from 
  lv_by_atc l -- 
inner join 
  visit_stats s  -- looking at visits that did atc 
    on l.visit_id=s.visit_id
    and atcs > 0 -- only looking at visits that atc
    and s.listing_views > 1
where 
  before_atc = 1 -- only look at everything before atc
group by all
order by 1 asc
