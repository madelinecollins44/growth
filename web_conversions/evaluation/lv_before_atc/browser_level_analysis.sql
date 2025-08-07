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

--------------------------------------------------------
-- GET LV BEFORE AND AFTER FIRST ATC
--------------------------------------------------------
with visit_w_atc as ( -- GRABS FIRST VISIT_ID WHERE ATC HAPPENS
select
  browser_id,
  min(visit_id) as first_atc_visit
from 
  etsy-data-warehouse-dev.madelinecollins.holder_table
where
  added_to_cart =1 
group by all 
)
, atc_seq_number as ( -- GRABS THE SEQ NUMBER + VISIT ID OF WHEN FIRST ATC OCCURRED 
select
  ht.browser_id,
  va.first_atc_visit as atc_visit,
  min(sequence_number) as atc_seq_number
from 
  etsy-data-warehouse-dev.madelinecollins.holder_table ht
inner join 
  visit_w_atc va 
    on va.browser_id=ht.browser_id
    and va.first_atc_visit=ht.visit_id
where
  added_to_cart =1
group by all 
)
select
  lv.platform,
  case 
    when lv.visit_id < f.atc_visit OR (lv.visit_id = f.atc_visit and lv.sequence_number < f.atc_seq_number) then 1 
    else 0 
  end as before_first_atc, 
  count(lv.sequence_number) as listing_views,
  count(distinct listing_id) as listings,
  count(distinct lv.visit_id) as visits
from 
  etsy-data-warehouse-dev.madelinecollins.holder_table lv
inner join 
  atc_seq_number f
    on lv.browser_id=f.browser_id
group by all 
order by 1,2 desc
