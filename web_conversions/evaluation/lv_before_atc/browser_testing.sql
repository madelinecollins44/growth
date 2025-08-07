----------------------------------------------------------------
-- TEST 1: testing holder_table
----------------------------------------------------------------
select 
  browser_id, 
  count(distinct visit_id) as visits,
  count(distinct visit_order) as visit_order_dist,
  count(sequence_number) as seq_count,
  count(visit_order) as visit_order_count,
from 
  etsy-data-warehouse-dev.madelinecollins.holder_table
group by all order by 2 desc limit 5
/* 
browser_id	visits	visit_order_dist	seq_count	visit_order_count
tAlCW0axYrDvqeljozEvH81B-LXA	568	2351	2351	2351
Chw6TcGWUt9gMea7espKpIBazr0H	551	950	950	950
VN9RjRvmwAgAruYAtEF7TYnZ7tOM	480	1331	1331	1331
i5csrSk8yFIMffNiPZceoWYLYY9I	465	1003	1003	1003
y12MGtoZMogqv3GnudIc6FSRPRyM	447	1195	1195	1195
*/

select * from etsy-data-warehouse-dev.madelinecollins.holder_table
where browser_id in ('tAlCW0axYrDvqeljozEvH81B-LXA') order by visit_id, sequence_number asc 
/* 
platform	browser_id	visit_id	start_datetime	sequence_number	listing_id	added_to_cart	visit_order
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753348679690.1	2025-07-24 09:17:59.000000 UTC	3	1900215809	0	1
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753348679690.1	2025-07-24 09:17:59.000000 UTC	60	4312537914	0	1
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753348679690.1	2025-07-24 09:17:59.000000 UTC	100	1867251491	0	1
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753348679690.1	2025-07-24 09:17:59.000000 UTC	138	4312537914	0	1
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753348679690.1	2025-07-24 09:17:59.000000 UTC	181	1761671467	0	1
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753348679690.1	2025-07-24 09:17:59.000000 UTC	222	4312537914	0	1
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753348679690.1	2025-07-24 09:17:59.000000 UTC	259	1900215809	0	1
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753348679690.1	2025-07-24 09:17:59.000000 UTC	296	1900215809	0	1
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753348890416.2	2025-07-24 09:21:30.000000 UTC	0	1877692016	0	2
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753348890416.2	2025-07-24 09:21:30.000000 UTC	38	1775090589	0	2
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753348890416.2	2025-07-24 09:21:30.000000 UTC	81	1658642321	0	2
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753348890416.2	2025-07-24 09:21:30.000000 UTC	110	1801664687	0	2
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753348890416.2	2025-07-24 09:21:30.000000 UTC	150	1898142887	0	2
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753348890416.2	2025-07-24 09:21:30.000000 UTC	194	1460977045	0	2
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753348890416.2	2025-07-24 09:21:30.000000 UTC	228	1477851501	0	2
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753348890416.2	2025-07-24 09:21:30.000000 UTC	262	1501746968	0	2
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753349110711.3	2025-07-24 09:25:10.000000 UTC	0	1649729921	0	3
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753349110711.3	2025-07-24 09:25:10.000000 UTC	45	1775176157	0	3
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753349110711.3	2025-07-24 09:25:10.000000 UTC	86	1683772439	0	3
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753349110711.3	2025-07-24 09:25:10.000000 UTC	129	1775176157	0	3
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753349110711.3	2025-07-24 09:25:10.000000 UTC	167	1548459523	0	3
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753349110711.3	2025-07-24 09:25:10.000000 UTC	209	1548820519	0	3
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753349110711.3	2025-07-24 09:25:10.000000 UTC	263	1548459523	0	3
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753349110711.3	2025-07-24 09:25:10.000000 UTC	296	1775176157	0	3
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753349110711.3	2025-07-24 09:25:10.000000 UTC	316	1649729921	0	3
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753349745228.4	2025-07-24 09:35:45.000000 UTC	0	4332294612	0	4
mobile_web	tAlCW0axYrDvqeljozEvH81B-LXA	tAlCW0axYrDvqeljozEvH81B-LXA.1753349766931.5	2025-07-24 09:36:06.000000 UTC	0	1863472783	0	5
*/

----------------------------------------------------------------
-- TEST 2: testing logic to get first atc moment for visit/ seq number
----------------------------------------------------------------
with visit_w_atc as (
select
  browser_id,
  min(visit_id) as first_atc_visit
from 
  etsy-data-warehouse-dev.madelinecollins.holder_table
where
  added_to_cart =1 
group by all 
)
-- , atc_seq_number as (
select
  ht.browser_id,
  va.first_atc_visit as atc_visit,
  min(sequence_number) as atc_seq_number
from 
  etsy-data-warehouse-dev.madelinecollins.holder_table ht
inner join 
  visit_w_atc va 
    on va.browser_id=ht.browser_id
    and va.first_atc_visit=ht.visit_id
where
  added_to_cart =1
group by all 
limit 5 
/* 
browser_id	atc_visit	atc_seq_number
30AEE65372134BF39EF867D7B99F	30AEE65372134BF39EF867D7B99F.1754066534858.1	2613
4F8E4A9752FF4A90AC7581EE1663	4F8E4A9752FF4A90AC7581EE1663.1754303737408.1	236
92BE2F1DEB10491E878D8837971F	92BE2F1DEB10491E878D8837971F.1753769566970.1	993
zdXggN5aS6WHnOHx456DcA	zdXggN5aS6WHnOHx456DcA.1753868065039.1	995
08BDCC1F69344A69BFEAB8346A96	08BDCC1F69344A69BFEAB8346A96.1754435311589.1	440
*/

select * from etsy-data-warehouse-dev.madelinecollins.holder_table where browser_id in ('92BE2F1DEB10491E878D8837971F') and added_to_cart = 1 order by visit_order, sequence_number asc
/* 
platform	browser_id	visit_id	start_datetime	sequence_number	listing_id	added_to_cart	visit_order
boe	30AEE65372134BF39EF867D7B99F	30AEE65372134BF39EF867D7B99F.1754066534858.1	2025-08-01 16:42:14.000000 UTC	2613	1446278113	1	54
boe	30AEE65372134BF39EF867D7B99F	30AEE65372134BF39EF867D7B99F.1754324369909.2	2025-08-04 16:19:29.000000 UTC	5483	1247010964	1	63
boe	30AEE65372134BF39EF867D7B99F	30AEE65372134BF39EF867D7B99F.1754330609634.3	2025-08-04 18:03:29.000000 UTC	297	1446278113	1	64
boe	30AEE65372134BF39EF867D7B99F	30AEE65372134BF39EF867D7B99F.1754330609634.3	2025-08-04 18:03:29.000000 UTC	1242	154083162	1	64
boe	30AEE65372134BF39EF867D7B99F	30AEE65372134BF39EF867D7B99F.1754351478478.1	2025-08-04 23:51:18.000000 UTC	280	1446278113	1	65

platform	browser_id	visit_id	start_datetime	sequence_number	listing_id	added_to_cart	visit_order
boe	4F8E4A9752FF4A90AC7581EE1663	4F8E4A9752FF4A90AC7581EE1663.1754303737408.1	2025-08-04 10:35:37.000000 UTC	236	4341059824	1	1

platform	browser_id	visit_id	start_datetime	sequence_number	listing_id	added_to_cart	visit_order
boe	92BE2F1DEB10491E878D8837971F	92BE2F1DEB10491E878D8837971F.1753769566970.1	2025-07-29 06:12:46.000000 UTC	993	4324179915	1	1
*/
