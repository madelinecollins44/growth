select
  platform,
  is_digital,
  top_category,
  is_personalizable, 
  case 
    when (l.price_usd/100) > 100 then 'high' 
    when (l.price_usd/100) > 30 then 'mid' 
    when (l.price_usd/100) <= 30 then 'low' 
  end as listing_price, -- uses same logic as segment
  count(distinct l.listing_id) as total_listings,
  count(distinct v.listing_id) as viewed_listings,
  count(sequence_number) as listing_views,
  sum(purchased_after_view) as purchases,
from 
  etsy-data-warehouse-prod.listing_mart.listings l
left join 
  etsy-data-warehouse-prod.analytics.listing_views v using (listing_id)
left join
  etsy-data-warehouse-prod.listing_mart.listing_attributes a
    on a.listing_id=l.listing_id
where 1=1
  and v._date between date('2025-03-01') and date('2025-03-15') -- two weeks before first reviews experiment was ramped 
  --and v._date between date('2025-06-10') and date('2025-06-24') -- two weeks after last reviews experiment was ramped 
  and platform in ('mobile_web','desktop')
group by all 
