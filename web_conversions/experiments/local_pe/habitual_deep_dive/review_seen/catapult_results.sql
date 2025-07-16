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
