-- How often is Read more in announcements clicked? How often is Reviews, About, and Shop policies clicked in tabs?
select
		beacon.event_name as event_name,
		(select value from unnest(beacon.properties.key_value) where key = "tab") as tab_clicked,
    count(visit_id) as clicks,
    count(distinct visit_id) as visits,
	from
		`etsy-visit-pipe-prod.canonical.visit_id_beacons`
	where
		date(_partitiontime) >= current_date-30
    and beacon.event_source in ('web')
	and
		beacon.event_name in ("shop_home_nav_clicked", 'shop_home','shop_home_announcement_view')
  group by all


--check using weblog.events
select 
  event_type,
  count(e.visit_id) as clicks,
  count(distinct e.visit_id) as visits,
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where
  v._date >= current_date-30
  and event_type in ("shop_home_nav_clicked", 'shop_home','shop_home_announcement_view')
  and v.platform in ('mobile_web','desktop')
group by all 
