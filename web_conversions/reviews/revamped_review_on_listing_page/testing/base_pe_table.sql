select bucketing_id, count(*) from etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_post_cattags group by all having count(*) = 15 order by 2 asc limit 5
/* 
bucketing_id	f0_
bKWZiJQwn1B48b5tqaHQPb0ZFgqi	340
BntJ8ePasWuSSRyrOTupfYcClH1m	306
0429950f4dda4b49ab8d61b80fba	291
cef979f025ba4c8fb00642c961d9	287
755cec8067934b3b88096f2d6dc0	274
ydJSw6ODfa5ryYWxnFXdNfx1gIvC	1
0A6qc_0VlfxaOyW9N2ios5oVb5m_	1
nQxWU2D_JCeL32R0Kq2HVwN_jbyB	1
FhkXNjxwSuzANTP9mZsh_hvlQCSU	1
1xGVPdPt5UcfyBvM6LeyMlIRea0o	1
PauNxj0k2hJijvTLSi9lnFVi7iN9	15
Dy1P49vyHMXpEKVco51-freTzb7D	15
ULc7lwM2Dql89BN1bB3douF5SbqR	15
HJneuFsz6QNhfS61ai8jFtBVg2jl	15
u9FNLP7rOgBzXqSd9c-IqVnsnjrg	15
*/

-- check to make sure the dates are after the bucketing date of 6/10
select visit_date, count(distinct visit_id) from etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_post_cattags where bucketing_id in ('HJneuFsz6QNhfS61ai8jFtBVg2jl') group by all order by 1 asc 
/* 
bKWZiJQwn1B48b5tqaHQPb0ZFgqi
visit_date	f0_
2025-06-20	12
2025-06-21	12
2025-06-22	1
2025-06-23	16
2025-06-24	45
2025-06-25	62
2025-06-26	61
2025-06-27	120
2025-06-28	10
2025-06-29	1

0A6qc_0VlfxaOyW9N2ios5oVb5m_
visit_date	f0_
2025-06-27	1

HJneuFsz6QNhfS61ai8jFtBVg2jl
visit_date	f0_
2025-06-11	1
2025-06-12	2
2025-06-13	1
2025-06-14	1
2025-06-19	3
2025-06-20	1
2025-06-26	1
2025-06-28	1
2025-06-29	1
2025-06-30	3
*/

select distinct bucketing_date from etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_post_cattags where bucketing_id in ('ULc7lwM2Dql89BN1bB3douF5SbqR') order by 1 asc  
-- 2025-06-20


-- select bucketing_id, visit_id, count(*) from etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_post_cattags group by all order by 3 desc limit 5
-- select bucketing_id, count(distinct bucketing_date) from etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_post_cattags group by all order by 2 desc limit 5
