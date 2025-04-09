/* this rollup only needs to be run once to backfill the table to look at buyer behavior over the last */

/* STEP 1: create table with all purchase info for mapped_user_ids since 2022 */ 
--- create large dataset for predicting cluster type starting in 2022 - 2024

-- create dataset with cluster info
create or replace table `etsy-data-warehouse-dev.madelinecollins.all_buyers` as (
with gms_2024 as (
select
  mapped_user_id,
  sum(gms_net) AS gms,
  count(distinct t.date) as n_purchase_days
from
  `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` t
where
  extract(year from t.date) = 2024
  and buyer_country = 'US'
group by 1
) 
, buyers_2024 as (
select distinct 
  case when gms >= (select distinct PERCENTILE_CONT(gms, 0.90) OVER() AS gms_2024_p90 from gms_2024) then 1 else 0 end as top_10_pct,
  case when buyer_segment = 'Habitual' then 1 else 0 end as is_habitual,
  a.mapped_user_id,
  a.n_purchase_days,
  a.gms
from gms_2024 a
  left join `etsy-data-warehouse-prod.rollups.buyer_segment_monthly` b 
  on a.mapped_user_id = b.mapped_user_id and b.as_of_date = date('2024-12-31')
) 
, transactions as (
select
	a.mapped_user_id,
	a.top_10_pct,
	a.n_purchase_days,
	a.gms as total_gms,
	t.transaction_id,
	date_trunc(t.date, month) as purch_month,
	date_trunc(t.date, quarter) as purch_quarter,
	t.listing_id,
	t.quantity,
	t.usd_subtotal_price,
	s.top_level_cat_new,
	s.second_level_cat_new,
	s.third_level_cat_new,
	s.is_digital,
	s.is_vintage,
	s.is_made_to_order,
	s.is_pod_listing,
	s.is_custo_perso,
	s.is_home_decor_and_improvement,
	s.is_kids_and_baby,
	s.is_supplies,
	s.is_perso_new,
	s.is_pod_expanded,
	CASE WHEN s.is_digital = 1 THEN "Digital"
     WHEN s.is_perso_new = 1 AND s.is_pod_expanded = 1 THEN "Perso POD"
     WHEN s.is_perso_new = 1 AND s.is_pod_expanded = 0 THEN "Perso Non-POD"
     WHEN s.is_pod_expanded = 1 THEN "Non-Perso POD"
     WHEN s.is_supplies = 1 OR s.top_level_cat_new = "craft_supplies_and_tools" THEN "Craft Supplies"
     WHEN s.is_vintage = 1 THEN "Vintage"
     WHEN s.top_level_cat_new = "jewelry" THEN "Jewelry"
     WHEN s.top_level_cat_new = "home_and_living" THEN "Home & Living"
     WHEN s.top_level_cat_new = "clothing" THEN "Clothing"
     WHEN s.top_level_cat_new = "art_and_collectibles" THEN "Art & Collectibles"
     ELSE "Other" END as inventory_segment,
	s.craftsmanship_score,
	s.craftsmanship_score_category
from 
  buyers_2024 a
    join `etsy-data-warehouse-prod.transaction_mart.all_transactions` t
      on t.mapped_user_id = a.mapped_user_id
left join
  `etsy-data-warehouse-prod.rollups.inventory_listing_segments` s
    on t.listing_id = s.listing_id
where 
  extract(year from date) = 2024
  and usd_subtotal_price > 0
)
, third_level_raw as (
select
	mapped_user_id,
	top_10_pct,
	n_purchase_days,
	total_gms,
	third_level_cat_new,
	sum(quantity) as n_items_purchased,
	sum(usd_subtotal_price) as gms_purchased
from 
  transactions
group by all 
) 
, total_sums as (
select 
	mapped_user_id,
	top_10_pct,
	n_purchase_days,
	total_gms,
	third_level_cat_new,
	n_items_purchased/sum(n_items_purchased) over(partition by mapped_user_id) as pi,
	gms_purchased/sum(gms_purchased) over(partition by mapped_user_id) as pii
from third_level_raw 
) 
, diversity_index as (
	select
		mapped_user_id,
		sum(-pi*ln(pi)) as diversity_index_items,
		sum(-pii*ln(pii)) as diversity_index_gms,
	from total_sums
	where pi > 0 and pii > 0
	group by all 
) 
select
t.mapped_user_id,
2024 as year,
top_10_pct,
n_purchase_days,
total_gms,
di.diversity_index_items,
di.diversity_index_gms,
count(distinct transaction_id) as n_transactions,
count(distinct purch_month) as n_purch_months,
count(distinct purch_quarter) as n_purch_quarter,
count(distinct top_level_cat_new) as n_categories,
count(distinct third_level_cat_new) as n_third_level,
sum(quantity) as n_items_purchased,
-- purchases over time
sum(case when purch_quarter = "2024-01-01" then quantity else 0 end)/sum(quantity) as q1_quantity,
sum(case when purch_quarter = "2024-01-01" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as q1_gms,

sum(case when purch_quarter = "2024-04-01" then quantity else 0 end)/sum(quantity) as q2_quantity,
sum(case when purch_quarter = "2024-04-01" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as q2_gms,

sum(case when purch_quarter = "2024-07-01" then quantity else 0 end)/sum(quantity) as q3_quantity,
sum(case when purch_quarter = "2024-07-01" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as q3_gms,

sum(case when purch_quarter = "2024-10-01" then quantity else 0 end)/sum(quantity) as q4_quantity,
sum(case when purch_quarter = "2024-10-01" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as q4_gms,


-- digital
sum(case when is_digital = 1 then quantity else 0 end)/sum(quantity) as digital_quantity,
sum(case when is_digital = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as digital_gms,
-- vintage
sum(case when is_vintage = 1 then quantity else 0 end)/sum(quantity) as vintage_quantity,
sum(case when is_vintage = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as vintage_gms,

-- made to order
sum(case when is_made_to_order = 1 then quantity else 0 end)/sum(quantity) as mto_quantity,
sum(case when is_made_to_order = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as mto_gms,
-- pod 
sum(case when is_pod_listing = 1 or is_pod_expanded = 1 then quantity else 0 end)/sum(quantity) as pod_quantity,
sum(case when is_pod_listing = 1 or is_pod_expanded = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as pod_gms,
-- custo/perso
sum(case when is_custo_perso = 1 or is_perso_new = 1 then quantity else 0 end)/sum(quantity) as custo_perso_quantity,
sum(case when is_custo_perso = 1 or is_perso_new = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as custo_perso_gms,

-- home decor
sum(case when is_home_decor_and_improvement = 1 then quantity else 0 end) /sum(quantity) as home_decor_impro_quantity,
sum(case when is_home_decor_and_improvement = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as home_decor_impro_gms,

-- kids & baby
sum(case when is_kids_and_baby = 1 then quantity else 0 end)/sum(quantity) as kids_baby_quantity,
sum(case when is_kids_and_baby = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as kids_baby_gms,

-- supplies
sum(case when is_supplies = 1 then quantity else 0 end)/sum(quantity) as supplies_quantity,
sum(case when is_supplies = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as supplies_gms,

-- wedding
sum(case when top_level_cat_new = "weddings" then quantity else 0 end)/sum(quantity) as wedding_quantity,
sum(case when top_level_cat_new = "weddings" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as wedding_gms,

-- jewelry
sum(case when top_level_cat_new = "jewelry" then quantity else 0 end)/sum(quantity) as jewelry_quantity,
sum(case when top_level_cat_new = "jewelry" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as jewelry_gms,

-- pet supplies
sum(case when top_level_cat_new = "pet_supplies" then quantity else 0 end)/sum(quantity) as pet_supplies_quantity,
sum(case when top_level_cat_new = "pet_supplies" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as pet_supplies_gms,

avg(coalesce(craftsmanship_score,0)) as avg_craftmanship,

-- emerging
sum(case when inventory_segment in ("Digital", "Perso POD", "Perso Non-POD", "Non-Perso POD") then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as emerging_gms


from transactions t
join diversity_index di 
on t.mapped_user_id = di.mapped_user_id

group by all
);


---- insert 2023 buyers
insert into `etsy-data-warehouse-dev.madelinecollins.all_buyers` (
with gms_2023 as (
select
  mapped_user_id,
  sum(gms_net) AS gms,
  count(distinct t.date) as n_purch_days
from
  `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` t
where
  extract(year from t.date) = 2023
  and buyer_country = 'US'
group by 1
) 
, buyers_2023 as (
select distinct 
  case when gms >= (select distinct PERCENTILE_CONT(gms, 0.90) OVER() AS gms_2023_p90 from gms_2023) then 1 else 0 end as top_10_pct,
  case when buyer_segment = 'Habitual' then 1 else 0 end as is_habitual,
  a.mapped_user_id,
  a.n_purch_days,
  a.gms
from 
  gms_2023 a
left join
  `etsy-data-warehouse-prod.rollups.buyer_segment_monthly` b 
  on a.mapped_user_id = b.mapped_user_id and b.as_of_date = date('2023-12-31')
) 
, transactions as (
select
	a.mapped_user_id,
	a.top_10_pct,
	a.n_purch_days as n_purchase_days,
	a.gms as total_gms,
	t.transaction_id,
	date_trunc(t.date, month) as purch_month,
	date_trunc(t.date, quarter) as purch_quarter,
	t.listing_id,
	t.quantity,
	t.usd_subtotal_price,
	s.top_level_cat_new,
	s.second_level_cat_new,
	s.third_level_cat_new,
	s.is_digital,
	s.is_vintage,
	s.is_made_to_order,
	s.is_pod_listing,
	s.is_custo_perso,
	s.is_home_decor_and_improvement,
	s.is_kids_and_baby,
	s.is_supplies,
	s.is_perso_new,
	s.is_pod_expanded,
	CASE WHEN s.is_digital = 1 THEN "Digital"
     WHEN s.is_perso_new = 1 AND s.is_pod_expanded = 1 THEN "Perso POD"
     WHEN s.is_perso_new = 1 AND s.is_pod_expanded = 0 THEN "Perso Non-POD"
     WHEN s.is_pod_expanded = 1 THEN "Non-Perso POD"
     WHEN s.is_supplies = 1 OR s.top_level_cat_new = "craft_supplies_and_tools" THEN "Craft Supplies"
     WHEN s.is_vintage = 1 THEN "Vintage"
     WHEN s.top_level_cat_new = "jewelry" THEN "Jewelry"
     WHEN s.top_level_cat_new = "home_and_living" THEN "Home & Living"
     WHEN s.top_level_cat_new = "clothing" THEN "Clothing"
     WHEN s.top_level_cat_new = "art_and_collectibles" THEN "Art & Collectibles"
     ELSE "Other" END as inventory_segment,
	s.craftsmanship_score,
	s.craftsmanship_score_category
from 
  buyers_2023 a
join 
  `etsy-data-warehouse-prod.transaction_mart.all_transactions` t
    on t.mapped_user_id = a.mapped_user_id
left join 
  `etsy-data-warehouse-prod.rollups.inventory_listing_segments` s
    on t.listing_id = s.listing_id
where 
  extract(year from date) = 2023
  and usd_subtotal_price > 0
)
, third_level_raw as (
select
	mapped_user_id,
	top_10_pct,
	n_purchase_days,
	total_gms,
	third_level_cat_new,
	sum(quantity) as n_items_purchased,
	sum(usd_subtotal_price) as gms_purchased
from 
  transactions
group by all 
) 
, total_sums as (
select 
		mapped_user_id,
		top_10_pct,
		n_purchase_days,
		total_gms,
		third_level_cat_new,
		n_items_purchased/sum(n_items_purchased) over(partition by mapped_user_id) as pi,
		gms_purchased/sum(gms_purchased) over(partition by mapped_user_id) as pii
from 
  third_level_raw 
) 
, diversity_index as (
select
		mapped_user_id,
		sum(-pi*ln(pi)) as diversity_index_items,
		sum(-pii*ln(pii)) as diversity_index_gms,
from 
  total_sums
where 
  pi > 0 and pii > 0
group by all 
) 
select
t.mapped_user_id,
2023 as year,
top_10_pct,
n_purchase_days,
total_gms,
di.diversity_index_items,
di.diversity_index_gms,
count(distinct transaction_id) as n_transactions,
count(distinct purch_month) as n_purch_months,
count(distinct purch_quarter) as n_purch_quarter,
count(distinct top_level_cat_new) as n_categories,
count(distinct third_level_cat_new) as n_third_level,
sum(quantity) as n_items_purchased,
-- purchases over time
sum(case when purch_quarter = "2023-01-01" then quantity else 0 end)/sum(quantity) as q1_quantity,
sum(case when purch_quarter = "2023-01-01" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as q1_gms,

sum(case when purch_quarter = "2023-04-01" then quantity else 0 end)/sum(quantity) as q2_quantity,
sum(case when purch_quarter = "2023-04-01" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as q2_gms,

sum(case when purch_quarter = "2023-07-01" then quantity else 0 end)/sum(quantity) as q3_quantity,
sum(case when purch_quarter = "2023-07-01" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as q3_gms,

sum(case when purch_quarter = "2023-10-01" then quantity else 0 end)/sum(quantity) as q4_quantity,
sum(case when purch_quarter = "2023-10-01" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as q4_gms,


-- digital
sum(case when is_digital = 1 then quantity else 0 end)/sum(quantity) as digital_quantity,
sum(case when is_digital = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as digital_gms,
-- vintage
sum(case when is_vintage = 1 then quantity else 0 end)/sum(quantity) as vintage_quantity,
sum(case when is_vintage = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as vintage_gms,

-- made to order
sum(case when is_made_to_order = 1 then quantity else 0 end)/sum(quantity) as mto_quantity,
sum(case when is_made_to_order = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as mto_gms,
-- pod 
sum(case when is_pod_listing = 1 or is_pod_expanded = 1 then quantity else 0 end)/sum(quantity) as pod_quantity,
sum(case when is_pod_listing = 1 or is_pod_expanded = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as pod_gms,
-- custo/perso
sum(case when is_custo_perso = 1 or is_perso_new = 1 then quantity else 0 end)/sum(quantity) as custo_perso_quantity,
sum(case when is_custo_perso = 1 or is_perso_new = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as custo_perso_gms,

-- home decor
sum(case when is_home_decor_and_improvement = 1 then quantity else 0 end) /sum(quantity) as home_decor_impro_quantity,
sum(case when is_home_decor_and_improvement = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as home_decor_impro_gms,

-- kids & baby
sum(case when is_kids_and_baby = 1 then quantity else 0 end)/sum(quantity) as kids_baby_quantity,
sum(case when is_kids_and_baby = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as kids_baby_gms,

-- supplies
sum(case when is_supplies = 1 then quantity else 0 end)/sum(quantity) as supplies_quantity,
sum(case when is_supplies = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as supplies_gms,

-- wedding
sum(case when top_level_cat_new = "weddings" then quantity else 0 end)/sum(quantity) as wedding_quantity,
sum(case when top_level_cat_new = "weddings" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as wedding_gms,

-- jewelry
sum(case when top_level_cat_new = "jewelry" then quantity else 0 end)/sum(quantity) as jewelry_quantity,
sum(case when top_level_cat_new = "jewelry" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as jewelry_gms,

-- pet supplies
sum(case when top_level_cat_new = "pet_supplies" then quantity else 0 end)/sum(quantity) as pet_supplies_quantity,
sum(case when top_level_cat_new = "pet_supplies" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as pet_supplies_gms,

avg(coalesce(craftsmanship_score,0)) as avg_craftmanship,

-- emerging
sum(case when inventory_segment in ("Digital", "Perso POD", "Perso Non-POD", "Non-Perso POD") then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as emerging_gms


from transactions t
join diversity_index di 
on t.mapped_user_id = di.mapped_user_id

group by all
);

---- insert 2022 buyers
insert into `etsy-data-warehouse-dev.madelinecollins.all_buyers` (
with gms_2022 as (
select
  mapped_user_id,
  sum(gms_net) AS gms,
  count(distinct t.date) as n_purch_days
from
  `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` t
where
  extract(year from t.date) = 2022
  and buyer_country = 'US'
group by 1
) 
, buyers_2022 as (
select distinct 
  case when gms >= (select distinct PERCENTILE_CONT(gms, 0.90) OVER() AS gms_2022_p90 from gms_2022) then 1 else 0 end as top_10_pct,
  case when buyer_segment = 'Habitual' then 1 else 0 end as is_habitual,
  a.mapped_user_id,
  a.n_purch_days,
  a.gms
from 
  gms_2022 a
left join 
  `etsy-data-warehouse-prod.rollups.buyer_segment_monthly` b 
    on a.mapped_user_id = b.mapped_user_id and b.as_of_date = date('2022-12-31')
) 
, transactions as (
	select
	a.mapped_user_id,
	a.top_10_pct,
	a.n_purch_days as n_purchase_days,
	a.gms as total_gms,
	t.transaction_id,
	date_trunc(t.date, month) as purch_month,
	date_trunc(t.date, quarter) as purch_quarter,
	t.listing_id,
	t.quantity,
	t.usd_subtotal_price,
	s.top_level_cat_new,
	s.second_level_cat_new,
	s.third_level_cat_new,
	s.is_digital,
	s.is_vintage,
	s.is_made_to_order,
	s.is_pod_listing,
	s.is_custo_perso,
	s.is_home_decor_and_improvement,
	s.is_kids_and_baby,
	s.is_supplies,
	s.is_perso_new,
	s.is_pod_expanded,
	CASE WHEN s.is_digital = 1 THEN "Digital"
     WHEN s.is_perso_new = 1 AND s.is_pod_expanded = 1 THEN "Perso POD"
     WHEN s.is_perso_new = 1 AND s.is_pod_expanded = 0 THEN "Perso Non-POD"
     WHEN s.is_pod_expanded = 1 THEN "Non-Perso POD"
     WHEN s.is_supplies = 1 OR s.top_level_cat_new = "craft_supplies_and_tools" THEN "Craft Supplies"
     WHEN s.is_vintage = 1 THEN "Vintage"
     WHEN s.top_level_cat_new = "jewelry" THEN "Jewelry"
     WHEN s.top_level_cat_new = "home_and_living" THEN "Home & Living"
     WHEN s.top_level_cat_new = "clothing" THEN "Clothing"
     WHEN s.top_level_cat_new = "art_and_collectibles" THEN "Art & Collectibles"
     ELSE "Other" END as inventory_segment,
	s.craftsmanship_score,
	s.craftsmanship_score_category
from buyers_2022 a
join `etsy-data-warehouse-prod.transaction_mart.all_transactions` t
on t.mapped_user_id = a.mapped_user_id
left join `etsy-data-warehouse-prod.rollups.inventory_listing_segments` s
on t.listing_id = s.listing_id
where extract(year from date) = 2022
and usd_subtotal_price > 0
)
, third_level_raw as (
	select
	mapped_user_id,
	top_10_pct,
	n_purchase_days,
	total_gms,
	third_level_cat_new,
	sum(quantity) as n_items_purchased,
	sum(usd_subtotal_price) as gms_purchased
	from transactions
	group by all 
) 
, total_sums as (
	select 
		mapped_user_id,
		top_10_pct,
		n_purchase_days,
		total_gms,
		third_level_cat_new,
		n_items_purchased/sum(n_items_purchased) over(partition by mapped_user_id) as pi,
		gms_purchased/sum(gms_purchased) over(partition by mapped_user_id) as pii
	from third_level_raw 
) 
, diversity_index as (
	select
		mapped_user_id,
		sum(-pi*ln(pi)) as diversity_index_items,
		sum(-pii*ln(pii)) as diversity_index_gms,
	from total_sums
	where pi > 0 and pii > 0
	group by all 
) 

select
t.mapped_user_id,
2022 as year,
top_10_pct,
n_purchase_days,
total_gms,
di.diversity_index_items,
di.diversity_index_gms,
count(distinct transaction_id) as n_transactions,
count(distinct purch_month) as n_purch_months,
count(distinct purch_quarter) as n_purch_quarter,
count(distinct top_level_cat_new) as n_categories,
count(distinct third_level_cat_new) as n_third_level,
sum(quantity) as n_items_purchased,
-- purchases over time
sum(case when purch_quarter = "2022-01-01" then quantity else 0 end)/sum(quantity) as q1_quantity,
sum(case when purch_quarter = "2022-01-01" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as q1_gms,

sum(case when purch_quarter = "2022-04-01" then quantity else 0 end)/sum(quantity) as q2_quantity,
sum(case when purch_quarter = "2022-04-01" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as q2_gms,

sum(case when purch_quarter = "2022-07-01" then quantity else 0 end)/sum(quantity) as q3_quantity,
sum(case when purch_quarter = "2022-07-01" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as q3_gms,

sum(case when purch_quarter = "2022-10-01" then quantity else 0 end)/sum(quantity) as q4_quantity,
sum(case when purch_quarter = "2022-10-01" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as q4_gms,


-- digital
sum(case when is_digital = 1 then quantity else 0 end)/sum(quantity) as digital_quantity,
sum(case when is_digital = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as digital_gms,
-- vintage
sum(case when is_vintage = 1 then quantity else 0 end)/sum(quantity) as vintage_quantity,
sum(case when is_vintage = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as vintage_gms,

-- made to order
sum(case when is_made_to_order = 1 then quantity else 0 end)/sum(quantity) as mto_quantity,
sum(case when is_made_to_order = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as mto_gms,
-- pod 
sum(case when is_pod_listing = 1 or is_pod_expanded = 1 then quantity else 0 end)/sum(quantity) as pod_quantity,
sum(case when is_pod_listing = 1 or is_pod_expanded = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as pod_gms,
-- custo/perso
sum(case when is_custo_perso = 1 or is_perso_new = 1 then quantity else 0 end)/sum(quantity) as custo_perso_quantity,
sum(case when is_custo_perso = 1 or is_perso_new = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as custo_perso_gms,

-- home decor
sum(case when is_home_decor_and_improvement = 1 then quantity else 0 end) /sum(quantity) as home_decor_impro_quantity,
sum(case when is_home_decor_and_improvement = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as home_decor_impro_gms,

-- kids & baby
sum(case when is_kids_and_baby = 1 then quantity else 0 end)/sum(quantity) as kids_baby_quantity,
sum(case when is_kids_and_baby = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as kids_baby_gms,

-- supplies
sum(case when is_supplies = 1 then quantity else 0 end)/sum(quantity) as supplies_quantity,
sum(case when is_supplies = 1 then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as supplies_gms,

-- wedding
sum(case when top_level_cat_new = "weddings" then quantity else 0 end)/sum(quantity) as wedding_quantity,
sum(case when top_level_cat_new = "weddings" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as wedding_gms,

-- jewelry
sum(case when top_level_cat_new = "jewelry" then quantity else 0 end)/sum(quantity) as jewelry_quantity,
sum(case when top_level_cat_new = "jewelry" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as jewelry_gms,

-- pet supplies
sum(case when top_level_cat_new = "pet_supplies" then quantity else 0 end)/sum(quantity) as pet_supplies_quantity,
sum(case when top_level_cat_new = "pet_supplies" then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as pet_supplies_gms,

avg(coalesce(craftsmanship_score,0)) as avg_craftmanship,

-- emerging
sum(case when inventory_segment in ("Digital", "Perso POD", "Perso Non-POD", "Non-Perso POD") then usd_subtotal_price else 0 end)/sum(usd_subtotal_price) as emerging_gms


from transactions t
join diversity_index di 
on t.mapped_user_id = di.mapped_user_id

group by all
);
