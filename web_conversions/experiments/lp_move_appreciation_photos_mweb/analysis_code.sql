with photo_reviews as (
select
  listing_id,
  sum(has_image) as images
from etsy-data-warehouse-prod.rollups.transaction_reviews
where has_review > 0
group by all 
order by 2 desc
)
, post_bucketing_views as (
select 
  lv.listing_id,
  count(distinct bl.bucketing_id) as browsers_that_viewed,
  count(lv.sequence_number) as listing_views,
  sum(purchased_after_view) as purchases
from
  etsy-bigquery-adhoc-prod._script7472bfed173f9e1e2d8ad0bb22386768877334ae.bucketing_listing bl -- all info from first listing unit was bucketed on 
left join
  etsy-data-warehouse-prod.analytics.listing_views lv
    on bl.bucketing_id=split(lv.visit_id, ".")[0] -- browser ids
    and bl.sequence_number <= lv.sequence_number -- all listing views bucketing moment and after 
where lv._date between date('2025-05-20') and date('2025-05-27') -- dates of the experiment 
group by all 
)
select
  case
    when images = 0 then '0'
    when images = 1 then '1'
    when images = 2 then '2'
    when images = 3 then '3'
    when images = 4 then '4'
    else '5+' 
  end as review_photos,
  count(distinct listing_id) as listings,
  sum(listing_views) as listing_views,
  sum(purchases) as purchases 
from 
  post_bucketing_views
left join 
  photo_reviews using (listing_id)
group by all 
order by 1 asc
