create or replace table `coral-firefly-411510.roman_raw.roman_sent` 
PARTITION BY DATE_TRUNC(date,YEAR)
CLUSTER BY word_count AS
select * from `roman_raw.roman_sent1`;
