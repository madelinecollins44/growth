------------------------------------------------------------------
-- TEST 1: do browsers match up w catapult 
------------------------------------------------------------------
select
  variant_id,
  count(distinct bucketing_id)
from etsy-data-warehouse-dev.madelinecollins.bucketing_listing
group by all 

select bucketing_id, count(*) from etsy-data-warehouse-dev.madelinecollins.bucketing_listing group by all order by 2 desc limit 5
-- all unique 

------------------------------------------------------------------
-- TEST 2: how many browsers are missing listing_ids
------------------------------------------------------------------
select
  count(distinct bucketing_id) as total_browsers,
  count(distinct case when listing_id is null then bucketing_id end) as browsers_wo_listings,
  count(distinct case when listing_id is not null then bucketing_id end) as browsers_w_listings,
  count(distinct case when listing_id is null then bucketing_id end)/count(distinct bucketing_id) as share_wo_listings,
from etsy-data-warehouse-dev.madelinecollins.bucketing_listing
/* total_browsers	browsers_wo_listings	browsers_w_listings	share_wo_listings
40968347	1647	40966700	4.02017684530938e-05 */
