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
