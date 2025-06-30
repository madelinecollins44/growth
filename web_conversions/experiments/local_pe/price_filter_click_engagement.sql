with event_counts as (
select
    beacon.event_name as event_name
    , visit_id
    , split(visit_id, ".")[0] as browser_ids
    , sequence_number
    , (select value from unnest(beacon.properties.key_value) where key = "price_min") as price_min
    , (select value from unnest(beacon.properties.key_value) where key = "price_max") as price_max
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons`
where 1=1
	and date(_partitiontime) between date('2025-04-22') and date('2025-06-25') -- dates the pe were run 
    and beacon.event_name in ("shop_home","shop_home_filter_dropdown_open","shop_home_filter_dropdown_engagement")
)
select
    event_name,
    concat(price_min,'-',price_max) as price_range,
    count(sequence_number) as events,
    count(distinct visit_id) as visits,
    count(distinct browser_ids) as browser_ids
from 
    event_counts
group by all 
