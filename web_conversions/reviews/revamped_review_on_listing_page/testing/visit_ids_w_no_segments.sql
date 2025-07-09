select 
  variant_id,
  platform,
  signed_in,
  new_visitor,
  engaged_w_reviews,
  converted,
  visit_id
from etsy-data-warehouse-dev.madelinecollins.segments_and_events  
group by all 
order by 3 asc 
limit 10 
/* 
variant_id	platform	signed_in	new_visitor	engaged_w_reviews	converted	visit_id
on						Rg48LA5LiVDDMOULUm7z_ia9QsTj.1751944941340.1
on						1Z_nxPn118JlKanfSabsqXRflHm-.1751942920225.1
on						MI3_Pyd2ICuVmuZGYSc6qApmm5ym.1751987251535.1
off						CO6XJdZWaS9PgZt0N26G7xXztNk-.1751931784172.1
on						blrdnh5F33bO4lIZvg4TMeBiutXc.1751989471793.1
on						7oHZ3o-klWUyfH3-EuVzRsoG14dk.1751948382187.1
off						zhQG7v5MuLQs6mPAoRlbPaGohaSK.1751940007669.4
off						1rE-PlAM-0GuizbMbQ0m7xyTA8Dl.1751946317833.1
on						j_82-ErHeCBILxHNG-Fh3WASXSkY.1751939054156.1
off						l1yqjHXdFKPfnmCJPvlmHQ129g9v.1751948038507.1
*/
