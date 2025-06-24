select
  case
    when countif(has_review > 0 and extract(date from transaction_date) >= current_date-365) = 0 then '0 reviews'
    when avg(case when extract(date from transaction_date) >= current_date-365 and has_review then star_rating else null end) is null then '0 rating in past year but has reviews'
    when round(avg(case when extract(date from transaction_date) >= current_date-365 and has_review then star_rating else null end)) = 5 then 'avg 5 stars rating'
    when round(avg(case when extract(date from transaction_date) >= current_date-365 and has_review then star_rating else null end)) = 4 then 'avg 4 stars rating'
    when round(avg(case when extract(date from transaction_date) >= current_date-365 and has_review then star_rating else null end)) = 3 then 'avg 3 stars rating'
    when round(avg(case when extract(date from transaction_date) >= current_date-365 and has_review then star_rating else null end)) = 2 then 'avg 2 stars rating'
    when round(avg(case when extract(date from transaction_date) >= current_date-365 and has_review then star_rating else null end)) = 1 then 'avg 1 star rating'
    else 'other'
  end review_group,
  listing_id,
from 
  etsy-data-warehouse-prod.rollups.transaction_reviews
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY 
      CASE
        WHEN COUNTIF(has_review = TRUE AND DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) = 0 THEN '0 reviews'
        WHEN AVG(CASE WHEN DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) AND has_review THEN star_rating END) IS NULL THEN '0 rating in past year but has reviews'
        WHEN ROUND(AVG(CASE WHEN DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) AND has_review THEN star_rating END)) = 5 THEN 'avg 5 stars rating'
        WHEN ROUND(AVG(CASE WHEN DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) AND has_review THEN star_rating END)) = 4 THEN 'avg 4 stars rating'
        WHEN ROUND(AVG(CASE WHEN DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) AND has_review THEN star_rating END)) = 3 THEN 'avg 3 stars rating'
        WHEN ROUND(AVG(CASE WHEN DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) AND has_review THEN star_rating END)) = 2 THEN 'avg 2 stars rating'
        WHEN ROUND(AVG(CASE WHEN DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) AND has_review THEN star_rating END)) = 1 THEN 'avg 1 star rating'
        ELSE 'Other'
      END
    ORDER BY RAND()
  ) <= 3;
