/* This experiment added a section ingress under the listing pagination on shop home. 
This analysis is meant to figure out if some channels responded better to this. 
-- desktop: https://atlas.etsycorp.com/catapult/1361091594266 (growth_regx.sh_section_ingresses_under_pagination_desktop)
-- mobile web: https://atlas.etsycorp.com/catapult/1361101148193 (growth_regx.sh_section_ingresses_under_pagination_mweb) */

-----
-- TESTING
--------
--make sure bucketing_id + visit are still unique
select visit_id, bucketing_id, count(*) from etsy-bigquery-adhoc-prod._script9f5b85181fb4f1b4d3812a20f1ee628f2085669a.xp_visits group by all order by 3 desc 

--find a browser w/ a high visit count to check visit order
select bucketing_id, count(visit_id) from etsy-bigquery-adhoc-prod._script9f5b85181fb4f1b4d3812a20f1ee628f2085669a.xp_visits group by all order by 2 desc 

--does ordering work? 
select * from etsy-bigquery-adhoc-prod._script9f5b85181fb4f1b4d3812a20f1ee628f2085669a.xp_visits where bucketing_id in ('eUu6shzIyoyHizRX4lFJZTUtm46n') order by visit_order asc
