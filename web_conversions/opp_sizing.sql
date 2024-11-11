------------------------------------------------------------
--BUYER SEGMENT
------------------------------------------------------------
-- begin
-- create or replace temp table buyer_segments as (select * from etsy-data-warehouse-prod.rollups.buyer_segmentation_vw where as_of_date >= current_date-60);
-- end 

select
  buyer_segment,
  count(distinct visit_id) as traffic,
  count(distinct case when converted > 0 then visit_id end) as converted_visits,
  sum(total_gms) as total_gms
from 
  etsy-data-warehouse-prod.weblog.visits v
left join 
  etsy-data-warehouse-prod.user_mart.user_mapping um  
    on v.user_id=um.user_id
left join 
   etsy-bigquery-adhoc-prod._script3576c1913cc22d4fc3fb94858e603229f9ebd6c6.buyer_segments bs
    on um.mapped_user_id=bs.mapped_user_id
where _date >= current_date-30
and v.platform in ('mobile_web','desktop')
group by all 

  --signed in vs signed out 
select
  case when user_id is not null or user_id != 0 then 'signed in' else 'signed out' end as user_status,
  count(distinct visit_id) as traffic,
  count(distinct case when converted > 0 then visit_id end) as converted_visits,
  sum(total_gms) as total_gms
from 
  etsy-data-warehouse-prod.weblog.visits v
where _date >= current_date-30
and v.platform in ('mobile_web','desktop')
group by all 
------------------------------------------------------------
--REPORTING CHANNEL
------------------------------------------------------------
select
case 
      when top_channel in ('direct') then 'Direct'
      when top_channel in ('dark') then 'Dark'
      when top_channel in ('internal') then 'Internal'
      when top_channel in ('seo') then 'SEO'
      when top_channel like 'social_%' then 'Non-Paid Social'
      when top_channel like 'email%' then 'Email'
      when top_channel like 'push_%' then 'Push'
      when top_channel in ('us_paid','intl_paid') then
        case
          when (second_channel like '%gpla' or second_channel like '%bing_plas') then 'PLA'
          when (second_channel like '%_ppc' or second_channel like 'admarketplace') then case
          when third_channel like '%_brand' then 'SEM - Brand' else 'SEM - Non-Brand'
          end
      when second_channel='affiliates' then 'Affiliates'
      when (second_channel like 'facebook_disp%' or second_channel like 'pinterest_disp%') then 'Paid Social'
      when second_channel like '%native_display' then 'Display'
      when second_channel in ('us_video','intl_video') then 'Video' else 'Other Paid' end
      else 'Other Non-Paid' 
      end as reporting_channel,
  count(distinct visit_id) as traffic,
  count(distinct case when converted > 0 then visit_id end) as converted_visits,
  sum(total_gms) as total_gms
from 
  etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30
and v.platform in ('mobile_web','desktop')
group by all 

------------------------------------------------------------
--LISTING CATEGORY
------------------------------------------------------------

------------------------------------------------------------
--PLATFORM
------------------------------------------------------------
select
  browser_platform,
  platform,
  count(distinct visit_id) as traffic,
  count(distinct case when converted > 0 then visit_id end) as converted_visits,
  sum(total_gms) as total_gms
from 
  etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30
group by all 

------------------------------------------------------------
--ENTRY VS EXIT POINTS
------------------------------------------------------------
--landing events
select
  landing_event,
  count(distinct visit_id) as traffic,
  count(distinct case when converted > 0 then visit_id end) as converted_visits,
  sum(total_gms) as total_gms
from 
  etsy-data-warehouse-prod.weblog.visits v
where _date >= current_date-30
and v.platform in ('mobile_web','desktop')
group by all 


--understanding traffic of site
with primary_events as (
select
  event_type,
  visit_id,
  count(visit_id) as pageviews
from 
  etsy-data-warehouse-prod.weblog.events
where 
  page_view =1 
  and _date >= current_date-30
group by all 
)
select
  e.event_type,
  count(distinct e.visit_id) as total_visits,
  sum(pageviews) as pageviews,
  sum(total_gms) as total_gms
from   
  primary_events e
left join 
  etsy-data-warehouse-prod.weblog.visits v
    using (visit_id)
where
  v._date >= current_date-30
  and v.platform in ('mobile_web','desktop')
group by all 
