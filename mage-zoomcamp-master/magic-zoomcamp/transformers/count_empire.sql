-- Docs: https://docs.mage.ai/guides/sql-blocks
-- total articles by year + 'roman empire' articles
--CREATE OR REPLACE table roman_raw.roman_count as
SELECT year, 
count(*) all_articles,
sum(case when lower(text) like '%roman empire%' then 1 else 0 end) roman_articles
FROM {{ df_1 }} 
group by year;