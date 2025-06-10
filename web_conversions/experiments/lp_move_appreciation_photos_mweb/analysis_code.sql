select
  case 
    when reviews = 0 then '0'  
    when reviews = 1 then '1'  
    when reviews = 2 then '2'  
    when reviews = 3 then '3'  
    when reviews = 4 then '4' 
    else '5+'
  end as photo_count,
  count(distinct bl.bucketing_id) as bucketed_units,
  count(distinct bl.segment_value) as listings,
  count(lv.sequence_number) as listing_views,
  sum(purchased_after_view) as purchases, 
from 
  etsy-bigquery-adhoc-prod._script719e520a9cef39b06b88b49724324a3998ec5543.bucketing_listing_ids bl -- listing_ids of bucketed units 
left join 
  (select 
    listing_id, 
    coalesce(sum(has_review),0) as reviews 
  from etsy-data-warehouse-prod.rollups.transaction_reviews 
  group by all) tr
    on bl.segment_value = cast(tr.listing_id as string)
left join 
  etsy-data-warehouse-prod.analytics.listing_views lv 
    on bl.segment_value = cast(lv.listing_id as string)
    and bl.bucketing_id = split(lv.visit_id, ".")[0] -- browser_id
    and bl.sequence_number= lv.sequence_number 
    and _date is not null 
group by all 
