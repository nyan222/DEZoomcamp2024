{{ config(materialized='table') }}


select 
    rs.uid,
    rs.word_count,
    rs.text,
    rs.year,
    rs.sentiment
from {{ ref('stg_roman_sent') }} as rs
