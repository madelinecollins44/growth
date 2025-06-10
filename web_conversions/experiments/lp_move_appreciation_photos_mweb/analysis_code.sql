select
  -- variant_id,
  date(bucketing_ts) as bucketing_date,
  count(distinct bucketing_id) as units,
  (sum(case when event_id in ('gms') then event_value end)/100) as total_gms, -- total gms, in cents originally 

  count(distinct case when event_id in ('backend_cart_payment') and event_value > 0 then bucketing_id end) as units_that_convert, -- Conversions 
  count(distinct case when event_id in ('backend_cart_payment') and event_value > 0 then bucketing_id end) / count(distinct bucketing_id) as conversion_rate, -- CR 

  (sum(case when event_id in ('gms') then event_value end)/100)/count(distinct bucketing_id) as total_gms_per_unit, -- gms per unit  

  sum(case when event_id in ('total_winsorized_gms') then event_value end)/ count(case when event_id in ('total_winsorized_gms') and event_value != 0 then event_id end) as winsorized_acbv, -- ACBV 
from 
  etsy-data-warehouse-prod.catapult_unified.aggregated_event_daily
where 
  experiment_id = "local_pe.q2_2025.buyer_trust_accelerator.browser"
  and variant_id in ('off') -- with all the 
group by all
order by 1 asc

