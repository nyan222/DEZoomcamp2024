{{ config(materialized='table') }}


select 
    rs.year,
    'all_articles' as article_type,
    rs.all_articles as article_count
from {{ ref('stg_roman_count') }} as rs

union all

select 
    rs.year,
    'roman_articles' as article_type,
    rs.roman_articles as article_count
from {{ ref('stg_roman_count') }} as rs