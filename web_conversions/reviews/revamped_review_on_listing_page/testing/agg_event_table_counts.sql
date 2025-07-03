---------------------------------------------------------------------------------------------------- 
-- Test to make sure engagement tables are counting correctly
----------------------------------------------------------------------------------------------------
select bucketing_id, visit_id, count(*) from etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_listing_engagements_agg group by all order by 3 desc limit 5
/* 
bucketing_id	visit_id	f0_
vdeAkP9amDjvKQ-s9CCcgGiXnSXz	vdeAkP9amDjvKQ-s9CCcgGiXnSXz.1750281408410.1	5400
3ELPn6nTEiZgjn8FHmnHzxTuyvu0	3ELPn6nTEiZgjn8FHmnHzxTuyvu0.1750306689015.1	4187
E1F4NhkjfLy2MQq8jZOBxE9Gf7sM	E1F4NhkjfLy2MQq8jZOBxE9Gf7sM.1750912057943.2	1
DogCXbF1_PskjX-EQ1Hx-BnaQGe5	DogCXbF1_PskjX-EQ1Hx-BnaQGe5.1750924315290.1	1
m4tlSe6zAazzga8X9znXtHDEnFbQ	m4tlSe6zAazzga8X9znXtHDEnFbQ.1749890743610.2	12
NGcMDkRQtPaliaH0XUtYdc7EWZPA	NGcMDkRQtPaliaH0XUtYdc7EWZPA.1749734982097.1	12
*/

select bucketing_id, count(distinct visit_id) from etsy-data-warehouse-dev.madelinecollins.browsers_in_pe_listing_engagements_agg group by all order by 2 asc limit 5
/* 
bucketing_id	f0_
bKWZiJQwn1B48b5tqaHQPb0ZFgqi	317
o1sNMYyMTad_Wvk15zWnyjSnPFrX	197
8_s0Mrg2dmp5oMpp4CXLFgl6Gskq	191
QPpPBm3ZR6vV9iSIaAkkXIBF6mfc	183
ZVoLmS3ASZCObNGJx6uklw	183
*/

select 
  event_type, 
  count(sequence_number) 
from etsy-data-warehouse-prod.weblog.events 
  where event_type in ('listing_page_reviews_seen','listing_page_reviews_container_top_seen','listing_page_review_engagement_frontend','listing_page_reviews_pagination','appreciation_photo_overlay_opened','sort_reviews','reviews_categorical_tag_clicked','reviews_categorical_tags_seen','listing_page_reviews_content_toggle_opened','view_listing')
  and visit_id in ('vdeAkP9amDjvKQ-s9CCcgGiXnSXz.1750281408410.1')
