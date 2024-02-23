# 1
[RisingWave is a distributed SQL streaming database that enables simple, efficient, and reliable processing of streaming data.](https://docs.risingwave.com/docs/current/intro/)

# 2
[RisingWave provides a PostgreSQL-style interaction experience for stream processing, greatly lowering the barrier to entry for using stream computing technologies.](https://docs.risingwave.com/docs/current/intro/)

# 3
[Easily create and run user-defined functions (UDF) on Apache Arrow. You can define functions in Rust, Python or JavaScript, run natively or on WebAssembly.](https://github.com/risingwavelabs/arrow-udf)

# 4
[RisingWave allows users to create cascading materialized views, meaning users can define materialized views on top of other materialized views.](https://docs.risingwave.com/docs/current/intro/)

# 5
[When there is new data entering from the upstream systems, RisingWave will directly consume the data and carry out incremental computations.](https://tutorials.risingwave.com/docs/basics/ingestion)

# 6
[RisingWave supports high-concurrency ad-hoc queries. By persisting data in remote object storage in real-time, users can dynamically configure the number of query nodes based on the workload, efficiently supporting business demands.](https://docs.risingwave.com/docs/current/intro/)<br>
[RisingWave supports ad-hoc queries with all these join types](https://tutorials.risingwave.com/docs/advanced/join/)

# 7
[The reason is that in stream processing, queries with nested loop join have excessively high complexity, leading to poor performance, and there are fewer real-world use cases for them, hence they are not supported.](https://tutorials.risingwave.com/docs/advanced/join/)

# 8
[To view the progress of a running CREATE MATERIALIZED VIEW, CREATE INDEX, or CREATE SINK statement, run the following command:

SELECT * FROM rw_catalog.rw_ddl_progress;](https://docs.risingwave.com/docs/current/view-statement-progress/)

# 9
[Explain is explain everywhere](https://docs.risingwave.com/docs/current/sql-explain/)

# 10
[To ingest data from external sources into RisingWave, you need to create a source (CREATE SOURCE) or a table with connector settings (CREATE TABLE) in RisingWave.](https://docs.risingwave.com/docs/current/data-ingestion/)

# 11
[RisingWave injects watermarks into the stream based on the defined expressions. For subsequent data that arrives later than the watermark, RisingWave filters and discards it.](https://tutorials.risingwave.com/docs/advanced/watermark/)
