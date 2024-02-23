{#
    This macro returns the full name of the sent field
#}

{% macro get_sentiment(sent) -%}

    case {{ dbt.safe_cast("sent", api.Column.translate_type("string")) }}  
        when 'pos' then 'Positive Sentiment'
        when 'neg' then 'Negative Sentiment'
        when 'neu' then 'Neutral Sentiment'
        else 'EMPTY'
    end

{%- endmacro %}