/*
create or replace table etsy-data-warehouse-dev.madelinecollins.holder_table as (
with unique_visits as (
  select
    split(visit_id, ".")[0] as browser_id,
    visit_id,
    start_datetime,
    platform,
    row_number() over (
      partition by split(visit_id, ".")[0]
      order by start_datetime, visit_id
    ) as visit_order
  from etsy-data-warehouse-prod.weblog.visits
  where _date >= current_date - 14
    and platform in ('boe', 'mobile_web', 'desktop')
)
 select  
    uv.platform,
    uv.browser_id, 
    lv.visit_id,
    uv.start_datetime,
    lv.sequence_number, 
    lv.listing_id, 
    lv.added_to_cart,
    uv.visit_order
  from etsy-data-warehouse-prod.analytics.listing_views lv
  inner join unique_visits uv using (visit_id)
  where lv._date >= current_date - 14
);
*/
