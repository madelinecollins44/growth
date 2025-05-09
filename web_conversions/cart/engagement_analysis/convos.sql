
-- how many convos are started postpurchase?
select 
  started_post_purchase,  
  convo_type,
  count(distinct conversation_id) as total_convos
  from
    `etsy-data-warehouse-prod.rollups.convo_transactions` a
  where date(convo_start_date) >= '2024-01-01' and date(convo_start_date) <= '2024-12-31'
  group by all 
  order by 1 desc


  
-- buyer intiiated convos
select 
  buyer_segment,
  avg(message_count) as avg_message_count,
  context_type,
  count(distinct conversation_id) as total_convos
from
  `etsy-data-warehouse-prod.rollups.convo_transactions` a
where 
  date(convo_start_date) >= current_date-365
  and started_post_purchase > 0
  and convo_type in ('buyer initiated, seller received')
group by all 
order by 1 desc



-- where do convos start from?
with message_info as (
  select    
a.visit_id,
b.user_id,
b.platform,
converted,
beacon.loc,
beacon.ref,
(select value from unnest(beacon.properties.key_value) where key = "conversation_id") as convo_id,
(select value from unnest(beacon.properties.key_value) where key = "conversation_message_id") as message_id,
(select value from unnest(beacon.properties.key_value) where key = "referring_type") as referring_type,
(select value from unnest(beacon.properties.key_value) where key = "first_message") as first_message,
from `etsy-visit-pipe-prod.canonical.visit_id_beacons` a
join `etsy-data-warehouse-prod.weblog.recent_visits` b
on a.visit_id = b.visit_id
where beacon.event_name = "backend_send_convo"
and date(_partitiontime) > current_date - 30
and _date > current_date - 30
and b.platform in ("desktop","mobile_web")
)
, extract_ref as (
  select
  platform,
  converted,
ref,
referring_type,
message_id,
first_message,
regexp_substr(ref, "listing\\/([^\\/\?]+)", 1, 1) as listing_id,
regexp_substr(ref, "shop\\/([^\\/\?]+)", 1, 1) as shop_name,
regexp_substr(ref, "purchases\\/([^\\/\?]+)", 1, 1) as purchase_id,
from message_info 
)
select
platform,
converted,
case when listing_id is not null then "listing"
     when shop_name is not null then "shop"
     when purchase_id is not null then "purchase"
     when ref like "%cart%" then "cart"
     else referring_type end,
count(distinct message_id)
from extract_ref
group by all 
order by 1,3 desc



-- add in post purchase data
with message_info as (
  select    
  started_post_purchase,
a.visit_id,
b.user_id,
b.platform,
converted,
beacon.loc,
beacon.ref,
(select value from unnest(beacon.properties.key_value) where key = "conversation_id") as convo_id,
(select value from unnest(beacon.properties.key_value) where key = "conversation_message_id") as message_id,
(select value from unnest(beacon.properties.key_value) where key = "referring_type") as referring_type,
(select value from unnest(beacon.properties.key_value) where key = "first_message") as first_message,
from `etsy-visit-pipe-prod.canonical.visit_id_beacons` a
join `etsy-data-warehouse-prod.weblog.recent_visits` b on a.visit_id = b.visit_id
left join etsy-data-warehouse-prod.rollups.convo_transactions c on cast((select value from unnest(a.beacon.properties.key_value) where key = "conversation_id")as int64)=c.conversation_id
where beacon.event_name = "backend_send_convo"
and date(_partitiontime) > current_date - 30
and _date > current_date - 30
and b.platform in ("desktop","mobile_web")
)
, extract_ref as (
  select
  platform,
  started_post_purchase,
  converted,
ref,
referring_type,
message_id,
first_message,
regexp_substr(ref, "listing\\/([^\\/\?]+)", 1, 1) as listing_id,
regexp_substr(ref, "shop\\/([^\\/\?]+)", 1, 1) as shop_name,
regexp_substr(ref, "purchases\\/([^\\/\?]+)", 1, 1) as purchase_id,
from message_info 
)
select
platform,
started_post_purchase,
converted,
case when listing_id is not null then "listing"
     when shop_name is not null then "shop"
     when purchase_id is not null then "purchase"
     when ref like "%cart%" then "cart"
     else referring_type end,
count(distinct message_id)
from extract_ref
group by all 
order by 1,2,4 desc
