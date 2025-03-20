--overall traffic 
select
  platform,
  count(distinct visit_id) as visits,
  count(distinct case when converted > 0 then visit_id end) as visits
from 
  etsy-data-warehouse-prod.weblog.visits v
where 
  v._date >= current_date-30
  and v.platform in ('mobile_web','desktop')
group by all

--traffic to shop home/ listing grid  
select
  platform,
  event_type,
  count(distinct visit_id) as visits,
  count(distinct case when converted > 0 then visit_id end) as visits
from 
  etsy-data-warehouse-prod.weblog.events e  
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date-30
  and v.platform in ('mobile_web','desktop')
  and event_type in ('shop_home', 'shop_home_listing_grid_seen','shop_home_listings_section_seen')
group by all

----browsers that do this at least once 
--traffic to shop home/ listing grid  
with agg as (
select
  platform,
  event_type,
  browser_id,
  max(case when event_type in ('shop_home') then 1 else 0 end) as shop_home_visit,
  max(case when event_type in ('shop_home_listing_grid_seen') then 1 else 0 end) as grid_visit,
  max(case when event_type in ('shop_home_listings_section_seen') then 1 else 0 end) as section_visit,
from 
  etsy-data-warehouse-prod.weblog.events e  
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date-30
  and v.platform in ('mobile_web','desktop')
  and event_type in ('shop_home', 'shop_home_listing_grid_seen','shop_home_listings_section_seen')
group by all
)
select
  platform,
  count(distinct browser_id) as browsers,
  count(distinct case when shop_home_visit > 0 then 1 else 0 end) as sh_browsers,
  count(distinct case when grid_visit > 0 then 1 else 0 end) as grid_browsers,  
  count(distinct case when section_visit > 0 then 1 else 0 end) as section_browsers,
from agg
group by all 
