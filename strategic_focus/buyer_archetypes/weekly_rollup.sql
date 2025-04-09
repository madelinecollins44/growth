/* STEP 1: delete and insert new 2025 data. 
this is needed bc the rollup looks at aggregate data from the 2025 year, so it needs to be reset eacht time */


/* STEP 2: run the actual model */
 create or replace table `etsy-data-warehouse-madelinecollins.semanuele.predictions` 
    as (
SELECT 
predicted_cluster,
mapped_user_id,
year, 
top_10_pct
FROM 
    ML.PREDICT(MODEL `etsy-data-warehouse-dev.madelinecollins.cluster_model`,
     (SELECT 
    n_purchase_days,
    total_gms,
    diversity_index_items,
    diversity_index_gms,
    n_transactions,
    n_purch_months,
    n_purch_quarter,
    n_categories,
    n_third_level,
    n_items_purchased,
    q1_gms,
    q2_gms,
    q3_gms,
    q4_gms,
    digital_gms,
    vintage_gms,
    mto_gms,
    pod_gms,
    custo_perso_gms,
    home_decor_impro_gms,
    kids_baby_gms,
    supplies_gms,
    wedding_gms,
    jewelry_gms,
    pet_supplies_gms,
    avg_craftmanship,
    emerging_gms,
    mapped_user_id,
    year,
    top_10_pct
FROM 
    `etsy-data-warehouse-dev.madelinecollins.all_buyers`))
);
