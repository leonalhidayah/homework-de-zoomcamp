# week 4 analytics engineering

## no 1
with the environtment variables set up:
- DBT_BIGQUERY_PROJECT set to myproject
- DBT_BIGQUERY_DATASET  set to my_nyc_tripdata

{{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }} refers to source_name and table_name

so the query should be:
``` sql
select * from myproject.my_nyc_tripdata.ext_green_taxi
```


## no 2
Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY

if the days_back given in command-line, that value will be used,
but if not then it use the default, or DAYS_BACK in env_var
but if not then use the default from env_var is 30

## no 3
dbt run --select models/staging/+

because it run all model in staging folder while fct_taxi_monthly_zone_revenue model is not in staging


## no 4
all the answer are true except Setting a value for DBT_BIGQUERY_STAGING_DATASET env var is mandatory, or it'll fail to compile.
because the macro provides a fallback to DBT_BIGQUERY_TARGET_DATASET if DBT_BIGQUERY_STAGING_DATASET is not set.


i'm sorry datatalks and team i can't answer about Serious SQL, it's too hard for me as a new in this field.
thanks u very much