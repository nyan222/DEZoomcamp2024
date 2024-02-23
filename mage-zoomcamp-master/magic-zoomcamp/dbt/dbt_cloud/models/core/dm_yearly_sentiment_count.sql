{{ config(materialized='table') }}

with sentdata as (
    select * from {{ ref('stg_roman_sent') }}
)
    select 
    year,
    sentiment,
    count(*) sentiment_count
    from sentdata
    group by 1,2
    union all
    select 
    2024 year,
    sentiment,
    count(*) sentiment_count
    from sentdata
    group by 2
    