with trans as (
select
	t.transaction_id
	,t.buyer_user_id
	,t.usd_subtotal_price
	,t.usd_price as item_price
	,t.quantity
	,t.listing_id
	,c.new_category as top_category
	,t.creation_tsz
from 
  etsy-data-warehouse-prod.transaction_mart.all_transactions t
join 
etsy-data-warehouse-prod.transaction_mart.all_transactions_categories c
  on t.transaction_id = c.transaction_id
  and t.listing_id = c.listing_id
where 
  extract(year from date(creation_tsz))>= 2022
)
select
top_category,
  -- case when item_price > 100 then 'high stakes' else 'low stakes' end as item_type,
  count(distinct transaction_id) as transactions
from trans  
group by all 

-- item_type	transactions
-- low stakes	1441582294
-- high stakes	54346938

-- top_category	transactions
-- paper_and_party_supplies	182448750
-- bath_and_beauty	33732855
-- jewelry	136594957
-- books_movies_and_music	22190298
-- pet_supplies	19477381
-- accessories	84033506
-- craft_supplies_and_tools	277731184
-- art_and_collectibles	162208922
-- home_and_living	281765880
-- electronics_and_accessories	34269978
-- shoes	4390984
-- weddings	44933090
-- bags_and_purses	22400609
-- toys_and_games	46393161
-- clothing	135863291
-- other	7494386
