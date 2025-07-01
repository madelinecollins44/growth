select
  v.visit_id,
  v.listing_id,
  count(distinct concat(v.visit_id,v.sequence_number)) as views,
  count(case when c.visit_id is not null then c.sequence_number end) as cat_tag_clicks,
from
    etsy-bigquery-adhoc-prod._scriptdebfea959961ca726d835451aa6d75b230e0f4c0.tag_info v
left join 
    etsy-bigquery-adhoc-prod._scriptdebfea959961ca726d835451aa6d75b230e0f4c0.tag_info c
    on v.visit_id = c.visit_id
    and v.listing_id = c.listing_id
    and c.event_name = 'reviews_categorical_tag_clicked'
where
    v.event_name = 'view_listing'
    and v.listing_id in ('1473578059') 
    and v.visit_id in ('skhNUdBPpsquEPkzwKG8JV2T2EOC.1749852245639.2')
group by all
/*
visit_id	listing_id	views	cat_tag_clicks
wFhdm1Ydq1gwIqZRyVZoBLMdgVVv.1749966995804.1	1596217277	715	0
rzp3eprh0C-BSHgjEEhfpoXlxdDN.1749964491879.1	902476416	690	0
u7tCxngaOvn-owgNOh4ShbDd1CPR.1749797389455.1	1595046602	667	0
tA_2rVL3_pMClNM5W9Z5UticBdLr.1749846081144.1	1315674517	667	0
RPyiGFs5TraRXCWKtis7Snypqega.1749796844952.1	1706594477	632	0
psSHVZmDPLJl3cMGqJiX0WBk-n40.1749949555337.1	1877533144	591	0
iD3kP9IHYHGz8yNzJoxLl2gSFkgH.1749986857561.1	4297592092	569	0
RPyiGFs5TraRXCWKtis7Snypqega.1750061103972.1	1706594477	568	0
QLLvIaxgK1h5uGk4DyHESwJKVSSV.1749910708477.1	668577814	549	0
RPyiGFs5TraRXCWKtis7Snypqega.1750004878646.2	1706594477	542	0
*/

-- select * from etsy-bigquery-adhoc-prod._scriptdebfea959961ca726d835451aa6d75b230e0f4c0.tag_info
-- where visit_id in ('wFhdm1Ydq1gwIqZRyVZoBLMdgVVv.1749966995804.1')	
-- and listing_id in ('1596217277')
-- and event_name in ('view_listing','reviews_categorical_tag_clicked')
-- order by sequence_number asc

select * from etsy-bigquery-adhoc-prod._scriptdebfea959961ca726d835451aa6d75b230e0f4c0.tag_info where event_name in ('reviews_categorical_tag_clicked')

--1473578059
--skhNUdBPpsquEPkzwKG8JV2T2EOC.1749852245639.2

select * from etsy-bigquery-adhoc-prod._scriptdebfea959961ca726d835451aa6d75b230e0f4c0.tag_info where listing_id in ('1473578059') and visit_id in ('skhNUdBPpsquEPkzwKG8JV2T2EOC.1749852245639.2')  order by sequence_number asc


select * from etsy-data-warehouse-prod.analytics.listing_views  where listing_id = 1473578059 and visit_id in ('skhNUdBPpsquEPkzwKG8JV2T2EOC.1749852245639.2')  and _date >= current_date- 30 order by sequence_number asc
