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
-- avg act / lv across platforms
--------------------------------------------------------
with browsers as (
select distinct browser_id from etsy-data-warehouse-dev.madelinecollins.holder_table where added_to_cart =1
)
, agg as (
select
  platform,
  browser_id,
  count(sequence_number) as listing_views,
  sum(added_to_cart) as atc,
  count(distinct visit_id) as visits,
from etsy-data-warehouse-dev.madelinecollins.holder_table
inner join browsers using (browser_id)
group by 1,2
)
select
  platform,
  count(distinct browser_id) as browsers,
  sum(listing_views) as total_lv,
  avg(listing_views) as avg_lv,
  sum(atc) as total_atc,
  avg(atc) as avg_atc,
  sum(visits) as total_visits,
  avg(visits) as avg_visits,
from 
  agg
group by 1

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
, agg as (
select
  lv.platform,
  -- case 
  --   when lv.visit_id < f.atc_visit OR (lv.visit_id = f.atc_visit and lv.sequence_number < f.atc_seq_number) then 1 
  --   else 0 
  -- end as before_first_atc, 
  lv.browser_id,
  -- count(distinct lv.browser_id) as browsers
  count(lv.sequence_number) as listing_views,
  count(distinct listing_id) as listings,
  count(distinct lv.visit_id) as visits,
from 
  etsy-data-warehouse-dev.madelinecollins.holder_table lv
inner join 
  atc_seq_number f
    on lv.browser_id=f.browser_id
where (lv.visit_id < f.atc_visit OR (lv.visit_id = f.atc_visit and lv.sequence_number < f.atc_seq_number))
group by all 
order by 1,2 desc
)
select 
  platform,
  count(distinct browser_id) as browsers,
  sum(listing_views) as total_lv,
  avg(listing_views) as avg_lv,
  sum(visits) as visits,
  avg(visits) as avg_visit
from agg
group by all 

--------------------------------------------------------
-- # of lv by browsers before atc
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
, agg as (
select
  lv.platform,
  -- case 
  --   when lv.visit_id < f.atc_visit OR (lv.visit_id = f.atc_visit and lv.sequence_number < f.atc_seq_number) then 1 
  --   else 0 
  -- end as before_first_atc, 
  lv.browser_id,
  -- count(distinct lv.browser_id) as browsers
  count(lv.sequence_number) as listing_views,
  count(distinct listing_id) as listings,
  count(distinct lv.visit_id) as visits,
from 
  etsy-data-warehouse-dev.madelinecollins.holder_table lv
inner join 
  atc_seq_number f
    on lv.browser_id=f.browser_id
where (lv.visit_id < f.atc_visit OR (lv.visit_id = f.atc_visit and lv.sequence_number < f.atc_seq_number))
group by all 
order by 1,2 desc
)
select 
  -- platform,
  case when listing_views<= 50 then cast(listing_views as string) else '51+' end as listing_views,
  count(distinct browser_id) as browsers,
from agg
group by all 
order by 1 asc


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
, agg as (
select
  lv.platform,
  -- case 
  --   when lv.visit_id < f.atc_visit OR (lv.visit_id = f.atc_visit and lv.sequence_number < f.atc_seq_number) then 1 
  --   else 0 
  -- end as before_first_atc, 
  lv.browser_id,
  -- count(distinct lv.browser_id) as browsers
  count(lv.sequence_number) as listing_views,
  count(distinct listing_id) as listings,
  count(distinct lv.visit_id) as visits,
from 
  etsy-data-warehouse-dev.madelinecollins.holder_table lv
inner join 
  atc_seq_number f
    on lv.browser_id=f.browser_id
where (lv.visit_id < f.atc_visit OR (lv.visit_id = f.atc_visit and lv.sequence_number < f.atc_seq_number))
group by all 
order by 1,2 desc
)
select 
  platform,
  case when listing_views<= 50 then cast(listing_views as string) else '51+' end as listing_views,
  count(distinct browser_id) as browsers,
from agg
where platform in ('mobile_web')
group by all 
order by 2 asc
