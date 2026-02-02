# RevoData's Technical Assessment

## Introduction

Marked a few items as TODO, because those should be implemented in the real-world scenario when time constraints are not so tight

### Koheesio

Used [Koheesio](https://github.com/Nike-Inc/koheesio) classes instead writing pure PySpark code to read / write / process the data because:
  - I am one of the co-owners and maintainers of this framework
  - I am very familiar with its capabilities
  - In the enterprise environment it makes more sense to have a shared library with established patterns for common tasks

### Raw (Bronze)

- The general assumption is that in enterprise environment there is a need to load multiple JSON, CSV files (instead of one time load as in this assessment). This makes autoloader the perfect solution: can handle huge volumes of data with low latency, supports streaming, schema evolution. Checkpoints will guarantee that each incoming file is only processed once.
- Using `APPEND` mode to ensure that all the data is available in the raw (bronze) object, which facilitates reprocessing and backfills in silver layer
- Not doing any processing in the raw stage to minimize the risk of pipeline failures if data and formats change over time
- Default `string` type for CSV / JSON data is also benefitial, as the data is not clean, and this way there is no risk of data loss

### Cleansed (Silver)

- Continue with the streaming approach as it will provide the necessary future proofing and flexibility
- `forEachBatch` is used because `MERGE` statement otherwise not supported, however it is crucial to update data over time, preserve uniqueness on the business key level, as well as facilitate the backfill
- Cleansed table are created in advance which allows tight controls over the schema and necessary table features. In real life enterprise scenario unity catalog objects should never be created directly via ETL notebook. Better approach is to define the objects and their evolution via dedicated DDL statements and deploy those via CICD using some database management system (e.g. Liquibase). Schema evolution of the above mentioned objects should also be done via database management and CICD
- Using UUIDs because in enterprise environment using the predetermined algorithm and set of columns will allow to generate identical UUIDs within different pipelines (even owned by different teams) and eliminates the need to use dimensional table to lookup the PK
- (*TODO*) Use timestamp (if available) or CDF to properly deduplicate rows within the `forEachBatch` function
- (*TODO*) Geo-spatial functionality (`st_`) were not available in the workspace / cluster type that I was using, hence storing latitude and longitude in separate decimal columns, ideally `st_geogfromtext(concat('point(', longitude, ' ', latitude, ')')) as location_geog` should be used