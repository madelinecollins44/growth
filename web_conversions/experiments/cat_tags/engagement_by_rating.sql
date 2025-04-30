
DECLARE config_flag_param STRING DEFAULT "growth_regx.lp_review_categorical_tags_desktop";
DECLARE start_date DATE;
DECLARE end_date DATE;

with bucketing_moment as ( -- grabs the first visit_id from bucketing 
select 
  bucketing_id,
  variant_id,
  (select id from unnest(associated_ids) where id_type = 3) AS visit_id,
  (select cast(id as int) from unnest(associated_ids) where id_type = 4) AS sequence_number
from 
  `etsy-data-warehouse-prod.catapult_unified.bucketing`
where 1=1
  -- and _date between start_date and end_date
  and experiment_id = 'growth_regx.lp_review_categorical_tags_desktop'
qualify row_number() over (partition by bucketing_id order by visit_id) = 1
)
, listing_view as ( -- gets listing_id associated with bucketing  
select  
  bucketing_id,
  listing_id 
from 
  bucketing_moment 
left join 
  etsy-data-warehouse-prod.weblog.events
  using (visit_id, sequence_number)
)
, cat_tag_clicks as (
select  
  bucketing_id
from 
  bucketing_moment bm
inner join 
  etsy-data-warehouse-prod.weblog.events e
    on bm.visit_id=e.visit_id
    and e.sequence_number >= bm.sequence_number -- engaged with the tag after bucketing 
where 
  event_type in ('reviews_categorical_tag_clicked')
  and _date >= current_date-30
)
, listing_rating as (
select
  listing_id,
  case 
    when coalesce(avg(rating),0) = 0 then '0'
    when coalesce(avg(rating),0) > 0 and coalesce(avg(rating),0) <= 1 then '1'
    when coalesce(avg(rating),0) > 1 and coalesce(avg(rating),0)<= 2 then '2'
    when coalesce(avg(rating),0) > 2 and coalesce(avg(rating),0)<= 3 then '3'
    when coalesce(avg(rating),0) > 3 and coalesce(avg(rating),0) <= 4 then '4'
    when coalesce(avg(rating),0) > 4 and coalesce(avg(rating),0) <= 5 then '5'
    else 'error'
  end as avg_rating
from
  `etsy-data-warehouse-prod.rollups.transaction_reviews` 
where 
  transaction_date >= timestamp_sub(current_timestamp(), interval 365 DAY)
group by all 
)
select
  avg_rating,
  count(distinct lv.bucketing_id) as bucketed_units,
  count(distinct ctc.bucketing_id) as bucketed_units_to_click,
from 
  listing_view lv
left join 
  listing_rating lr
    on cast(lv.listing_id as string) = cast(lr.listing_id as string)
left join 
  cat_tag_clicks ctc 
    on lv.bucketing_id=ctc.bucketing_id
group by all 
order by 1 asc
