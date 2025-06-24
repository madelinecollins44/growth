select 
  variant_id,
  -- e.visit_id,
  -- bucketing_id,
  v.listing_id,
  count(v.sequence_number) as views,
  count(case when event_name in ('view_listing') then e.sequence_number end) as listing_views, 
  sum(added_to_cart) as atc,
  sum(purchased_after_view) as purchase,
from 
  etsy-data-warehouse-prod.analytics.listing_views  v
inner join
  etsy-data-warehouse-dev.madelinecollins.beacons_events  e
    on e.visit_id =  v.visit_id
    and e.sequence_number =  v.sequence_number
    and e.listing_id= cast(v.listing_id as string)
    and event_name in ('view_listing')
where 
    _date between date('2025-06-13') and date('2025-06-22')  -- this will be within time of experiment
  and e.listing_id in ('1828303364', '4301728473', '1859859907', '779626793', '4321242971', '1885420599', '4322426709', '1058117011', '666567895','1806904333', '1801002655', '1685734088','1794114928')
group by all 
order by 3,2 desc

