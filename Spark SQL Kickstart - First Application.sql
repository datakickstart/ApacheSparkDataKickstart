-- Databricks notebook source
-- MAGIC %md
-- MAGIC # First Spark SQL Application using NYC Taxi data
-- MAGIC To build our first Spark SQL application we will use easily accessible New York Taxi data. I use this because it's available from Databricks, Azure Synapse, and for download from the web. We can start small but also scale to a large volume with this sample dataset when we are read.

-- COMMAND ----------

DROP TABLE IF EXISTS yellow_trip_sql_source;

CREATE TABLE yellow_trip_sql_source
USING CSV
OPTIONS(path "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz", header "true", inferSchema "true");

-- COMMAND ----------

CREATE OR REPLACE TABLE yellow_trip_sql_transformed AS
SELECT 
  *,
  regexp_replace(substring(tpep_pickup_datetime,1,7), '-', '_') year_month,
  to_date(tpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss') as pickup_dt,
  to_date(tpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss') as dropoff_dt,
  tip_amount/total_amount as tip_pct
FROM yellow_trip_sql_source
LIMIT 1000

-- COMMAND ----------

DROP TABLE IF EXISTS zone_sql_source;

CREATE TABLE zone_sql_source
USING CSV
OPTIONS(path "/databricks-datasets/nyctaxi/taxizone/taxi_zone_lookup.csv", header "true", inferSchema "true")


-- COMMAND ----------

CREATE OR REPLACE TABLE yellow_trips_sample_managed
AS
SELECT * EXCEPT(taxi_zone.LocationID)
FROM yellow_trip_sql_transformed trip
  LEFT JOIN zone_sql_source taxi_zone
    ON trip.PULocationID = taxi_zone.LocationID

-- COMMAND ----------

SELECT sum(total_amount), zone FROM yellow_trips_sample_managed group by zone

-- COMMAND ----------

DESCRIBE FORMATTED yellow_trips_sample_managed

-- COMMAND ----------

CREATE OR REPLACE TABLE yellow_trips_sample LOCATION "/tmp/datasets/datakickstart/yellow_trips_sample"
AS
SELECT * EXCEPT(taxi_zone.LocationID)
FROM yellow_trip_sql_transformed trip
  LEFT JOIN zone_sql_source taxi_zone
    ON trip.PULocationID = taxi_zone.LocationID

-- COMMAND ----------

DESCRIBE FORMATTED yellow_trips_sample

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Alternate Read & Write (meant for local environment)
-- MAGIC If you need this data local, you can download just one month of Yellow trip data plus the Taxi Zone Lookup Table. Data can be found at https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
-- MAGIC
-- MAGIC Bash commands to download two files:
-- MAGIC ```bash
-- MAGIC mkdir -p /tmp/datasets/nyctaxi/taxizone
-- MAGIC cd /tmp/datasets/nyctaxi/taxizone
-- MAGIC wget https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv
-- MAGIC mv taxi+_zone_lookup.csv taxi_zone_lookup.csv
-- MAGIC
-- MAGIC mkdir -p /tmp/datasets/nyctaxi/tables/nyctaxi_yellow
-- MAGIC cd /tmp/datasets/nyctaxi/tables/nyctaxi_yellow
-- MAGIC wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-01.parquet
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Spark and Delta versions
-- MAGIC As of May 9, 2023: Spark version 3.4 is not yet compatible with Delta Lake release. Use Spark version 3.3.* with Delta Lake version 2.3.0.  
-- MAGIC
-- MAGIC To run within `spark-sql` shell locally, add the following configs to the command to configure delta lake files. Note: `spark.sql.warehouse.dir` config is optional for defining where to save tables by default.
-- MAGIC ```
-- MAGIC spark-sql --packages io.delta:delta-core_2.12:2.3.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.sql.warehouse.dir=/tmp/delta-warehouse"
-- MAGIC ```

-- COMMAND ----------

DROP TABLE IF EXISTS yellow_trip_sql_source;

CREATE TABLE yellow_trip_sql_source
USING PARQUET
LOCATION "/tmp/datasets/nyctaxi/tables/nyctaxi_yellow";

-- COMMAND ----------

DROP TABLE IF EXISTS yellow_trip_sql_transformed;

CREATE TABLE yellow_trip_sql_transformed
USING DELTA 
AS
SELECT 
  *,
  regexp_replace(substring(tpep_pickup_datetime,1,7), '-', '_') year_month,
  to_date(tpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss') as pickup_dt,
  to_date(tpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss') as dropoff_dt,
  tip_amount/total_amount as tip_pct
FROM yellow_trip_sql_source
LIMIT 1000;

-- COMMAND ----------

DROP TABLE IF EXISTS zone_sql_source;

CREATE TABLE zone_sql_source
USING CSV
OPTIONS(path "/tmp/datasets/nyctaxi/taxizone/taxi_zone_lookup.csv", header "true", inferSchema "true");

-- COMMAND ----------

DROP TABLE IF EXISTS yellow_trips_sample_managed;

CREATE TABLE yellow_trips_sample_managed
USING DELTA
AS
SELECT trip.*, taxi_zone.Borough, taxi_zone.Zone, taxi_zone.service_zone
FROM yellow_trip_sql_transformed trip
  LEFT JOIN zone_sql_source taxi_zone
    ON trip.PULocationID = taxi_zone.LocationID;

-- COMMAND ----------

SELECT * FROM yellow_trips_sample_managed
