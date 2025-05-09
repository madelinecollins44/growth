select 
  started_post_purchase,  
  convo_type,
  count(distinct conversation_id) as total_convos
  from
    `etsy-data-warehouse-prod.rollups.convo_transactions` a
  where date(convo_start_date) >= '2024-01-01' and date(convo_start_date) <= '2024-12-31'
  group by all 
  order by 1 desc

-- buyer intiiated convos
select 
  buyer_segment,
  avg(message_count) as avg_message_count,
  context_type,
  count(distinct conversation_id) as total_convos
from
  `etsy-data-warehouse-prod.rollups.convo_transactions` a
where 
  date(convo_start_date) >= current_date-365
  and started_post_purchase > 0
  and convo_type in ('buyer initiated, seller received')
group by all 
order by 1 desc
