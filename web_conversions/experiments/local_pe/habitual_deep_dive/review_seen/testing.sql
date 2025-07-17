----------------------------------------------------------------------------------------
-- CHECKING HABITUAL VS NON HABIUTAL REVIEWS 
----------------------------------------------------------------------------------------
select 
  variant_id, 
  case when lower(buyer_segment) in ('habitual') then 1 else 0 end as habitual_segment,
  count(distinct bucketing_id), 
  count(distinct case when top_reviews_events > 0 then bucketing_id end) as browsers_w_top_seen, 
  count(distinct case when mid_reviews_events > 0 then bucketing_id end) as browsers_w_mid_seen, 
  sum(top_reviews_events) as top_reviews_events, 
  sum(mid_reviews_events) as mid_reviews_events,
  max(top_reviews_events) as max_top_reviews_events, 
  max(mid_reviews_events) as max_mid_reviews_events
from 
  etsy-bigquery-adhoc-prod._scriptf559414066a34ea7573bf7d307f319636261725a.browsers_with_key_event group by all 

----------------------------------------------------------------------------------------
-- CHECKING SEEN EVENTS FOR BROWSERS 
----------------------------------------------------------------------------------------
SELECT
  bucketing_id,
 top_reviews_events
FROM
  etsy-bigquery-adhoc-prod._script62fa58361ece06f40681dab645e289b397a057c7.xp_khm_agg_events_by_unit AS e 
-- where top_reviews_events =5
GROUP BY ALL
order by 2 desc limit 5

/* 
bucketing_id	top_reviews_events
2Mit18UUsX3WiP-OV4qBw-TJnkRx	11923
xFnmny-LfBvvBOSscZkGr96z3VTo	10971
3ctzT-ofyXOUb8kc50pS-kFG9rrl	10749
ur8KMfRdGT5kjT7iZ7B2FibNJV1M	7608
rWWbnCB_uRa3FCP5_qDhjETMEHmn	7599
bucketing_id	top_reviews_events
rTjRzvuQ-oE_9FrnGpA7kwQg8gaw	5
iVeN08BCyqApoi1UNHJ84h73LuOr	5
Nl24P_8qc8q_wu0ia6Yhh-A9CP_U	5
uHiZgzEQSjyGoIlgvE3yHZYG6jl4	5
C2j8RNBwnTgAkOe58sUnTUMrgryo	5
*/

select * from etsy-bigquery-adhoc-prod._scriptaf6e115b9c8ad80de9149827b51cf4eb197a45e6.ab_first_bucket where bucketing_id in ('xFnmny-LfBvvBOSscZkGr96z3VTo')

select 
  count(sequence_number) 
from 
  etsy-data-warehouse-prod.weblog.events 
where 
  split(visit_id, ".")[0] in ('xFnmny-LfBvvBOSscZkGr96z3VTo')
  and event_type in ('listing_page_reviews_container_top_seen')
  and timestamp_millis(epoch_ms) >= ('2025-05-13 07:42:26.124000 UTC')
  --bucketing_id, timestamp, views
  --2Mit18UUsX3WiP-OV4qBw-TJnkRx,2025-06-03 09:04:38.224000 UTC, 11399
  --xFnmny-LfBvvBOSscZkGr96z3VTo,2025-05-13 07:42:26.124000 UTC, 
  --C2j8RNBwnTgAkOe58sUnTUMrgryo, 2025-06-17 06:53:40.784000 UTC,5

