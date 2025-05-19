
select
  case 
    when regexp_contains(lower(review), r'\b(ship(ping)?|delivery|arrive|arrival|fast|transit|dispatch|speedy|estimated time|prompt|ETA|receive)\b') then 1
    else 0
  end as mentions_shipping,
  top_category,
  avg(rating) as avg_rating,
  count(distinct transaction_id)
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
where 
  has_review > 0 
  and language in ('en')
  and date(transaction_date) >= current_date-365
group by all 
order by 1,2 desc

  -- REGEXP_CONTAINS(LOWER(your_column), r'\b((fast|quick|on[- ]time|prompt|speed(y|ily)|efficient|earl(y|ier))\b.*\b(ship(ping)?|deliver(ed|y)?|arriv(e|al|ing)?|transit|dispatch|receive(d)?)\b|\b(ship(ping)?|deliver(ed|y)?|arriv(e|al|ing)?|transit|dispatch|receive(d)?)\b.*\b(fast|quick|on[- ]time|prompt|speed(y|ily)|efficient|earl(y|ier))\b)') AS said_positive_shipping

-- SELECT *,
  -- REGEXP_CONTAINS(LOWER(your_column), r'\b((slow|late|delay(ed)?|never arrived|long wait|terrible|bad|poor|awful|missing|lost|didn[’'\']?t (arrive|come|ship|receive))\b.*\b(ship(ping)?|deliver(ed|y)?|arriv(e|al|ing)?|transit|dispatch|receive(d)?)\b|\b(ship(ping)?|deliver(ed|y)?|arriv(e|al|ing)?|transit|dispatch|receive(d)?)\b.*\b(slow|late|delay(ed)?|never arrived|long wait|terrible|bad|poor
