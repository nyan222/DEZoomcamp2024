create or replace table `coral-firefly-411510.roman_raw.roman_words`  AS
select * from {{ df_1 }};

