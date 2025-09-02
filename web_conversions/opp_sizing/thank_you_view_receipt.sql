select
  platform,
  case when v.user_id is not null or v.user_id != 0 then 'signed_in' else 'signed_out' end as buyer_type,
  count(distinct visit_id) as visits,
  count(distinct case when event_type in ('thank_you') then visit_id end) as thank_you_visits,
  count(distinct case when event_type in ('view_receipt_modal_from_thank_you_module') then visit_id end) as view_reciept_visits,
from 
  etsy-data-warehouse-prod.weblog.events e
inner join
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 1=1
  and v._date >= current_date-30
  and platform in ('mobile_web','desktop')
group by 1,2 
order by 1,2 desc
