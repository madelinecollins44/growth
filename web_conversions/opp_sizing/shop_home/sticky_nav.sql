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
  and event_type in ('shop_home', 'shop_home_listing_grid_seen')
group by all
