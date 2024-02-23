{{ config(materialized='view') }}
 

select
    cast(year as integer) as year,
    cast(all_articles as integer) as all_articles,
    cast(roman_articles as integer) as roman_articles
    
from {{ source('staging','roman_count') }}
--where rn = 1

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
--{% if var('is_test_run', default=true) %}

  --limit 100

--{% endif %}