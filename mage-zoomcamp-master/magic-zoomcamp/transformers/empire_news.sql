create or replace table `coral-firefly-411510.roman_raw.roman_news` as
select news.id
,cast(news.date as date) date
,news.edition
,news.page
,news.file_name
,news.word_count
,news.text 
,news.year 
from (select * , extract( year from cast(date as date)) year 
from `roman_raw.all_news`) news
where lower(text) like '%roman empire%';
