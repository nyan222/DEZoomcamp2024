{{ config(materialized='table') }}

with sentdata as (
    select * from {{ ref('stg_roman_sent') }}
)
    select 
    year,
    cast(round(avg(word_count)) as integer) as avg_wordcount
    from sentdata
    group by 1
    union all
    select 
    2024 year,
    cast(round(avg(word_count)) as integer) as avg_wordcount
    from sentdata
    