# run the following in the terminal to log into bq
gcloud auth application-default login

## setting up r 
library(bigrquery)
library(DBI)

# bq project setup
billing <- 'etsy-bigquery-adhoc-prod'

# Authenticate to bq
bq_auth()

# pull in relevant data
## acbv
sql <- "select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_acbv`;"
tb <- bq_project_query(billing, sql)
df <- bq_table_download(tb,page_size=1000) 
treat_f <- df[df$variant_id == "on", ]
control_f <- df[df$variant_id == "off", ]
t.test(treat_f$event_count, control_f$event_count)

## order value
sql <- "select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_order_value`;"
tb <- bq_project_query(billing, sql)
df <- bq_table_download(tb,page_size=1000) 
treat_f <- df[df$variant_id == "on", ]
control_f <- df[df$variant_id == "off", ]
t.test(treat_f$event_count, control_f$event_count)

## conversion 
sql <- "select * from `etsy-data-warehouse-dev.madelinecollins.all_units_events_browser_level_conversion`;"
tb <- bq_project_query(billing, sql)
df <- bq_table_download(tb, page_size = 1000)
treat <- df[df$variant_id == "on", ]
control <- df[df$variant_id == "off", ]
t.test(treat$event_count, control$event_count)



