-- NUMBER OF LISTING VIEWS AMONG VISITS THAT ADDED TO CART 
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
, visit_level as (
select
  platform,
  visit_id, 
  case when sequence_number >= f.sequence_number then 1 else 0 end as after_atc,
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
  listing_views,
  count(distinct visit_id) as visits,
from 
  visit_level
inner join 
  (select distinct visit_id from visit_level where after_atc = 1) -- looking at visits that did atc 
    using (visit_id)
where 
  after_atc =0 -- only look at everything before atc
