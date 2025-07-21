/* OVERALL RECREATED RESULTS */

SELECT
  variant_id,
  -- case when lower(buyer_segment) in ('habitual') then 1 else 0 end as habitual_browser,
  -- case when mid_reviews_events > 0 then 1 else 0 end as saw_mid_reviews,
  case when top_reviews_events > 0 then 1 else 0 end as saw_top_reviews,
  -- mid_reviews_events,
  COUNT(bucketing_id) AS browsers,
  -- metrics
  SUM(e.winsorized_gms) as winsorized_gms,
  SAFE_DIVIDE(COUNTIF(e.orders > 0), COUNT(bucketing_id)) AS conversion_rate,
  -- SAFE_DIVIDE(COUNTIF(e.bounced_visits > 0), COUNT(bucketing_id)) AS bounce_rate,
  SAFE_DIVIDE(COUNTIF(e.atc_count > 0), COUNT(bucketing_id)) AS pct_with_atc,
  -- SAFE_DIVIDE(COUNTIF(e.checkout_start_count > 0), COUNT(bucketing_id)) AS pct_with_checkout_start,
  -- SAFE_DIVIDE(SUM(e.engaged_visits), COUNT(bucketing_id)) AS mean_engaged_visits,
  -- SAFE_DIVIDE(SUM(e.visits), COUNT(bucketing_id)) AS mean_visits,
  SAFE_DIVIDE(SUM(e.orders), COUNTIF(e.orders > 0)) AS ocb,
  SAFE_DIVIDE(SUM(e.completed_checkouts), COUNT(bucketing_id)) AS orders_per_browser,
  -- SAFE_DIVIDE(SUM(e.page_count), COUNT(bucketing_id)) AS pages_per_browser,
  SAFE_DIVIDE(SUM(e.winsorized_gms), COUNTIF(e.completed_checkouts > 0)) AS winsorized_acbv,
  SAFE_DIVIDE(SUM(e.winsorized_order_value_sum), SUM(e.completed_checkouts)) AS winsorized_aov,
  --browser counts
  COUNTIF(e.orders > 0) AS converted_browsers,
  COUNTIF(e.atc_count > 0) AS atc_browsers
FROM
  etsy-bigquery-adhoc-prod._script62fa58361ece06f40681dab645e289b397a057c7.xp_khm_agg_events_by_unit AS e 
WHERE lower(buyer_segment) in ('habitual')
GROUP BY ALL
ORDER BY 1, 2,3 desc


/* RECREATED RESULTS BY NUMBER OF REVIEW SEEN EVENTS */
SELECT
  variant_id,
  -- case when lower(buyer_segment) in ('habitual') then 1 else 0 end as habitual_browser,
  case 
    when top_reviews_events = 1 then '1'
    when top_reviews_events = 2 then '2'
    when top_reviews_events = 3 then '3'
    when top_reviews_events = 4 then '4'
    when top_reviews_events = 5 then '5'
    when top_reviews_events = 6 then '6'
    when top_reviews_events = 7 then '7'
    when top_reviews_events = 8 then '8'
    when top_reviews_events = 9 then '9'
    when top_reviews_events = 10 then '10'
    when top_reviews_events > 10 and top_reviews_events <=15 then '11-15'
    when top_reviews_events > 15 and top_reviews_events <=20 then '16-20'
    when top_reviews_events > 20 and top_reviews_events <=30 then '21-30'    
    when top_reviews_events > 30 and top_reviews_events <=40 then '31-40' 
    when top_reviews_events > 40 and top_reviews_events <=50 then '41-50'
    when top_reviews_events > 50 then '51+'
    else '0'
  end as top_reviews_events_group,
  COUNT(bucketing_id) AS browsers,
  -- metrics
  SUM(e.winsorized_gms) as winsorized_gms,
  SAFE_DIVIDE(COUNTIF(e.orders > 0), COUNT(bucketing_id)) AS conversion_rate,
  -- SAFE_DIVIDE(COUNTIF(e.bounced_visits > 0), COUNT(bucketing_id)) AS bounce_rate,
  SAFE_DIVIDE(COUNTIF(e.atc_count > 0), COUNT(bucketing_id)) AS pct_with_atc,
  -- SAFE_DIVIDE(COUNTIF(e.checkout_start_count > 0), COUNT(bucketing_id)) AS pct_with_checkout_start,
  -- SAFE_DIVIDE(SUM(e.engaged_visits), COUNT(bucketing_id)) AS mean_engaged_visits,
  -- SAFE_DIVIDE(SUM(e.visits), COUNT(bucketing_id)) AS mean_visits,
  SAFE_DIVIDE(SUM(e.orders), COUNTIF(e.orders > 0)) AS ocb,
  SAFE_DIVIDE(SUM(e.completed_checkouts), COUNT(bucketing_id)) AS orders_per_browser,
  -- SAFE_DIVIDE(SUM(e.page_count), COUNT(bucketing_id)) AS pages_per_browser,
  SAFE_DIVIDE(SUM(e.winsorized_gms), COUNTIF(e.completed_checkouts > 0)) AS winsorized_acbv,
  SAFE_DIVIDE(SUM(e.winsorized_order_value_sum), SUM(e.completed_checkouts)) AS winsorized_aov,
  --browser counts
  COUNTIF(e.orders > 0) AS converted_browsers,
  COUNTIF(e.atc_count > 0) AS atc_browsers
FROM
  etsy-bigquery-adhoc-prod._script62fa58361ece06f40681dab645e289b397a057c7.xp_khm_agg_events_by_unit AS e 
WHERE lower(buyer_segment) in ('habitual')
GROUP BY ALL
ORDER BY 1, 2 asc



/* ALL SEGMENTS IN TREATMENT GROUP*/
  SELECT
  variant_id,
  buyer_segment,
  -- case when mid_reviews_events > 0 then 1 else 0 end as saw_mid_reviews,
  case when top_reviews_events > 0 then 1 else 0 end as saw_top_reviews,
  -- mid_reviews_events,
  COUNT(bucketing_id) AS browsers,
  -- metrics
  SUM(e.winsorized_gms) as winsorized_gms,
  SAFE_DIVIDE(COUNTIF(e.orders > 0), COUNT(bucketing_id)) AS conversion_rate,
  -- SAFE_DIVIDE(COUNTIF(e.bounced_visits > 0), COUNT(bucketing_id)) AS bounce_rate,
  SAFE_DIVIDE(COUNTIF(e.atc_count > 0), COUNT(bucketing_id)) AS pct_with_atc,
  -- SAFE_DIVIDE(COUNTIF(e.checkout_start_count > 0), COUNT(bucketing_id)) AS pct_with_checkout_start,
  -- SAFE_DIVIDE(SUM(e.engaged_visits), COUNT(bucketing_id)) AS mean_engaged_visits,
  -- SAFE_DIVIDE(SUM(e.visits), COUNT(bucketing_id)) AS mean_visits,
  SAFE_DIVIDE(SUM(e.orders), COUNTIF(e.orders > 0)) AS ocb,
  SAFE_DIVIDE(SUM(e.completed_checkouts), COUNT(bucketing_id)) AS orders_per_browser,
  -- SAFE_DIVIDE(SUM(e.page_count), COUNT(bucketing_id)) AS pages_per_browser,
  SAFE_DIVIDE(SUM(e.winsorized_gms), COUNTIF(e.completed_checkouts > 0)) AS winsorized_acbv,
  SAFE_DIVIDE(SUM(e.winsorized_order_value_sum), SUM(e.completed_checkouts)) AS winsorized_aov,
  --browser counts
  COUNTIF(e.orders > 0) AS converted_browsers,
  COUNTIF(e.atc_count > 0) AS atc_browsers
FROM
  etsy-bigquery-adhoc-prod._scriptb5f7cb041c3195bf654f02d1bddc3e589685a40e.xp_khm_agg_events_by_unit AS e 
-- WHERE lower(buyer_segment) in ('habitual')
WHERE variant_id in ('on')
GROUP BY ALL
ORDER BY 2,1,3 desc

