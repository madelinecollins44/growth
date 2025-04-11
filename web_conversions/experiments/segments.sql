-- VIEW LISTING MULTIPLE TIMES: this will show if at 
with unit_listing_views as (
    -- browser bucketed tests
    select 
      {{input_run_date}} as _date,
      split(lv.visit_id, ".")[0] as bucketing_id, 
      1 as bucketing_id_type, 
      lv.listing_id,
      count(lv.sequence_number)) as listing_views
    from `etsy-data-warehouse-prod.analytics.listing_views` lv
    where lv._date between DATE_SUB({{input_run_date}}, INTERVAL 14 DAY) and {{input_run_date}} 
    group by all
    union all 
    -- user bucketed_tests 
    select 
      {{input_run_date}} as _date,
      cast(v.user_id as string) as bucketing_id, 
      2 as bucketing_id_type, 
      lv.listing_id,
      count(lv.sequence_number)) as listing_views
    from `etsy-data-warehouse-prod.analytics.listing_views` lv
    left join `etsy-data-warehouse-prod.weblog.visits` v 
      on v.visit_id = lv.visit_id
    where lv._date between DATE_SUB({{input_run_date}}, INTERVAL 14 DAY) and {{input_run_date}} 
    and v._date = {{input_run_date}}
    group by all
)
select 
  _date,            
  bucketing_id, 
  bucketing_id_type,
  case 
      -- when recent_listing_views = 0 then '0' (default segment value will be applied)
      when max(listing_views) = 1 and '1'
      when max(listing_views) = 2 and '2'
      when max(listing_views) = 3 and '3'
      when max(listing_views) = 4 and '4'
     else '5+' end as segment_value
  from unit_listing_views
