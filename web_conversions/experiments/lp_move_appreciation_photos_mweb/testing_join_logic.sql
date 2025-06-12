-- find bucketing id with high sequence number
select * from etsy-bigquery-adhoc-prod._scriptdee81035175fba82e21a15db89d2ad31b2dc12b4.bucketing_listing  order by sequence_number desc limit 10 
  /* 
  bucketing_id	bucketing_ts	listing_id	visit_id	sequence_number	listing_ts	abs_time_between
yRvFwecOKGCFqanAVq5-eWE5fxUx	2025-05-23 14:38:47.150000 UTC	4301886650	yRvFwecOKGCFqanAVq5-eWE5fxUx.1748007592132.1	17765	2025-05-23 14:38:47.150000 UTC	0
JWFCNySJMzrv35QFBIH42-Vdsxfx	2025-05-20 22:15:54.888000 UTC	1868313418	JWFCNySJMzrv35QFBIH42-Vdsxfx.1747774129335.1	17161	2025-05-20 22:15:54.889000 UTC	0
wn3XcleW9HI13fyejVjcfbwBzSwt	2025-05-22 06:42:04.238000 UTC	945078320	wn3XcleW9HI13fyejVjcfbwBzSwt.1747885466680.2	16865	2025-05-22 06:42:04.239000 UTC	0
phOnEkOXpcYY7sJdppYuBKOgUQ3y	2025-05-20 22:17:29.870000 UTC	1748973982	phOnEkOXpcYY7sJdppYuBKOgUQ3y.1747770624599.1	16841	2025-05-20 22:17:29.871000 UTC	0
eTPrwuqbJGdwYH6Zteykpuo6anMz	2025-05-24 21:21:56.606000 UTC	253305826	eTPrwuqbJGdwYH6Zteykpuo6anMz.1748107824201.1	16539	2025-05-24 21:21:56.606000 UTC	0
pnS1bbF2sNvmICUWi6sFvb2E9lxK	2025-05-20 22:21:21.959000 UTC	1027408878	pnS1bbF2sNvmICUWi6sFvb2E9lxK.1747772802263.2	16266	2025-05-20 22:21:21.959000 UTC	0
yfvEqslIRaORPkoHmrli3w	2025-05-26 23:12:52.661000 UTC	1749760089	yfvEqslIRaORPkoHmrli3w.1748283633320.1	16205	2025-05-26 23:12:52.663000 UTC	0
mbNM0Q6gCsLNJnkUL4wLHDebqIK6	2025-05-20 22:16:27.578000 UTC	1267993898	mbNM0Q6gCsLNJnkUL4wLHDebqIK6.1747771787005.1	15870	2025-05-20 22:16:27.580000 UTC	0
89q5W5p_VWSdyOK4EDVZOgSOlPTx	2025-05-20 22:30:35.918000 UTC	4303109396	89q5W5p_VWSdyOK4EDVZOgSOlPTx.1747770170879.2	15484	2025-05-20 22:30:35.919000 UTC	0
hx3VlDULCdeGzAwaD5QnPO5qOoB-	2025-05-20 22:16:40.826000 UTC	1898359563	hx3VlDULCdeGzAwaD5QnPO5qOoB-.1747774211072.2	14533	2025-05-20 22:16:40.838000 UTC	0
  */

--check to see that joins are happening on lower sequence numbers after that first bucketing 
select
	date(_partitiontime) as _date,
  case 
    when v.visit_id = bl.visit_id and v.sequence_number >= bl.sequence_number then 1 -- if within the same visit AND on bucketing sequence number or after 
    when v.visit_id > bl.visit_id then 1 -- after the bucketing visit_id
    else 0 
  end as after_bucketing_flag,
	min(v.sequence_number),
from
	`etsy-visit-pipe-prod.canonical.visit_id_beacons` v
inner join 
  etsy-bigquery-adhoc-prod._scriptdee81035175fba82e21a15db89d2ad31b2dc12b4.bucketing_listing bl -- only looking at browsers in the experiment 
    on bl.bucketing_id= split(v.visit_id, ".")[0] -- joining on browser_id
    and v.visit_id >= bl.visit_id -- everything that happens on bucketing moment and after (cant do sequence number bc there is only one)
where
	date(_partitiontime) between date('2025-05-20') and date('2025-05-27') -- dates of the experiment 
	and beacon.event_name in ("view_listing")
  and split(v.visit_id, ".")[0] in ('yRvFwecOKGCFqanAVq5-eWE5fxUx')
    /* 
 _date	after_bucketing_flag	f0_
2025-05-23	0	2
2025-05-23	1	17765
2025-05-24	1	59
2025-05-25	1	284
2025-05-26	1	4
2025-05-27	1	43
  */

group by all 
order by 1,3 asc
