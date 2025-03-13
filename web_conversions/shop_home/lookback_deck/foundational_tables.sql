 select
 _DATE
  , event_name
  , browser_id
  , (select value from unnest(properties.map) where key = "shop_shop_id") as shop_id
  , (select value from unnest(properties.map) where key = "shop_id") as seller_user_id
  , count((select value from unnest(properties.map) where key = "sequence_number")) as pageviews
from 
  etsy-visit-pipe-prod.canonical.beacon_main_2024_01 
where
  event_name in ('shop_home')
  and _DATE is not null   
group by all 
limit 40






   
declare i int64 default 1;
declare dates ARRAY<date>;

declare end_dt date;
declare load_dt date;
declare start_dt date;
declare earliest_beacon_data date;
declare latest_metrics_data_dt date;


set earliest_beacon_data = (
  select 
    date(parse_datetime("%Y%m%d%H", min(nullif(partition_id, "__NULL__")))) 
  from 
      `etsy-visit-pipe-prod.canonical.INFORMATION_SCHEMA.PARTITIONS`
  where
      table_name = 'beacon_main_2024_01');

set latest_metrics_data_dt = (
  select 
    date(parse_datetime("%Y%m%d%H", min(nullif(partition_id, "__NULL__")))) 
  from 
      `etsy-visit-pipe-prod.canonical.INFORMATION_SCHEMA.PARTITIONS`
  where
      table_name = 'beacon_main_2025_');


/*
 * Generate an array of dates to be processed. It should use the earliest beacon data
 * available if the table is empty, otherwise it's the latest data available in the
 * metrics table + 1 day, or the current date -1 day
 */
set end_dt = current_date()-1;
set start_dt = least(coalesce(latest_metrics_data_dt+1, earliest_beacon_data, current_date()-1), end_dt); --least is use for cases where latest_metrics_data_dt+1 is greater than end_dt
set dates = generate_date_array(start_dt, end_dt, interval 1 day);

create table if not exists `etsy-data-warehouse-dev.madelinecollins.shop_home_visits` (
  event_name string
  , visit_id string
  , shop_id string
  , seller_user_id string 
  , pageviews int64
)
-- partition by _partitiondate
;

/*
 * The beacon tables are quite large and we want to avoid querying more than one day at a time.
 * The logic below loops through the array of dates and does single day operations.
 */
loop

  set load_dt = dates[ordinal(i)];

  -- make sure we are not duplicating data
  -- delete from `etsy-data-warehouse-dev.madelinecollins.shop_home_visits` where _partitiondate = load_dt;

  insert into `etsy-data-warehouse-dev.madelinecollins.shop_home_visits` (
  _partitiondate
  , event_name 
  , visit_id 
  , shop_id 
  , seller_user_id
  , pageviews 
  )
    
    select
      _partitiondate
      , event_name
      , visit_id
      , (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id
      , (select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id
      , count(sequence_number) as pageviews
    from 
      `etsy-visit-pipe-prod.canonical.beacon_main_2024_`
    where 
      date(_partitiontime) = load_dt
      and event_name in ('shop_home')
    group by all;


set i = i+1;

  if i > array_length(dates) then
    leave;
  end if;

end loop;

