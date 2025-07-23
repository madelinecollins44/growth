SELECT
  variant_id,
  case when 
    lower(buyer_segment) in ('active', 'new', 'high_potential') then 'other'
    else lower(buyer_segment)
  end as buyer_segment, 
  avg(top_reviews_events) as avg_top_reviews_events,
  avg(mid_reviews_events) as avg_mid_reviews_events,
  COUNT(bucketing_id) AS browsers,
  -- metrics
  SUM(e.winsorized_gms) as winsorized_gms,
  SAFE_DIVIDE(COUNTIF(e.orders > 0), COUNT(bucketing_id)) AS conversion_rate,
FROM
  etsy-bigquery-adhoc-prod._scriptd692ac1c65a33f3f4653d120cd1d580d4e51d290.xp_khm_agg_events_by_unit AS e 
-- WHERE lower(buyer_segment) in ('habitual')
GROUP BY ALL
ORDER BY 1, 2 asc
