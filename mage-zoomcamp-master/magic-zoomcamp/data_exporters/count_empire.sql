create or replace table `coral-firefly-411510.roman_raw.roman_count` as 
SELECT year, 
count(*) all_articles,
sum(case when lower(text) like '%roman empire%' then 1 else 0 end) roman_articles
FROM (select * , extract( year from cast(date as date)) year 
from `roman_raw.all_news`) 
group by year;
SELECT year, 
count(*) all_articles,
sum(case when lower(text) like '%roman empire%' then 1 else 0 end) roman_articles
FROM (select * , extract( year from cast(date as date)) year 
from `roman_raw.all_news`) 
group by year;
