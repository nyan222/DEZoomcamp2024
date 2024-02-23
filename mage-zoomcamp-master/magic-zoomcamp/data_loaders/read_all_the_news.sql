-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE OR REPLACE EXTERNAL TABLE `roman_raw.external_roman`
OPTIONS (
  format = PARQUET,
  uris = ['gs://roman_empire_zoomcamp/*.parquet']
);

CREATE OR REPLACE TABLE `roman_raw.all_news` as
select * from `roman_raw.external_roman`;

;
