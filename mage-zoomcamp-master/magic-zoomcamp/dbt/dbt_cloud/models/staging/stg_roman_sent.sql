{{ config(materialized='view') }}
 
with sentdata as 
(
  select *,
    row_number() over(partition by id,date,page) as rn
  from {{ source('staging','roman_sent') }}

)
select
   -- identifiers
    {{ dbt_utils.generate_surrogate_key(['id', 'date', 'page']) }} as uid,    
    {{ dbt.safe_cast("id", api.Column.translate_type("string")) }} as id,
    
   -- dates
    cast(date as date) as date,
    
    -- unuseful info
    edition,
    page,
    file_name,
    
    -- useful info
    cast(word_count as integer) as word_count,
    text,
    cast(year as integer) as year,
    coalesce({{ dbt.safe_cast("sent", api.Column.translate_type("string")) }},'empty') as sent,
    {{ get_sentiment('sent') }} as sentiment
from sentdata
--where rn = 1

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
--{% if var('is_test_run', default=true) %}

  --limit 100

--{% endif %}