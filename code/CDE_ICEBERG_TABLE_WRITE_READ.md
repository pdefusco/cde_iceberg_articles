# ICEBERG TABLE WRITE and READ with Spark in Cloudera Data Engineering

## Objective

This article provides an introduction to the Iceberg Merge Into using Spark SQL in Cloudera Data Engineering (CDE). CDE provides native Apache Iceberg Table Format support in its Spark Runtimes. Iceberg supports the Merge Into command, a great alternative to Spark upserts which will save you time and effort when running ETL jobs at scale.

## Abstract

CDP Data Engineering (CDE) is the only cloud-native service purpose-built for enterprise data engineering teams. Building on Apache Spark, Data Engineering is an all-inclusive data engineering toolset that enables orchestration automation with Apache Airflow, advanced pipeline monitoring, visual troubleshooting, and comprehensive management tools to streamline ETL processes across enterprise analytics teams.

Apache Iceberg is an open table format format for huge analytic datasets. It provides features that, coupled with Spark as the compute engine, allows you to build data processing pipelines with dramatic gains in terms of scalability, performance, and overall developer productivity.

Iceberg is natively supported by CDE. Any time a CDE Spark Job or Session is created, Iceberg dependencies are automatically set in the SparkSession without any need for configurations. As a CDP User, the CDE Data Engineer can thus create, read, modify, and interact with Iceberg tables as allowed by Ranger policies, whether these were created in Cloudera Data Warehouse (CDW), DataHub, or Cloudera AI (CML).

In this tutorial you will create a CDE Session and interact with Apache Iceberg tables using PySpark.

## Requirements

* CDE Virtual Cluster of type "All-Purpose" running in CDE Service with version 1.22 or above, and Spark version 3.2 or above.
* An installation of the CDE CLI is recommended but optional. In the steps below you will create the CDE Session using the CLI, but you can alternatively launch one using the UI.

#### Create CDE Files Resource

```
cde resource create --name myFiles --type files
cde resource upload --name myFiles --local-path resources/cell_towers_1.csv --local-path resources/cell_towers_2.csv
```

#### Launch CDE Session & Run Spark Commands

```
cde session create --name icebergSessionCDE --type pyspark --mount-1-resource myFiles
cde session interact --name icebergSessionCDE
```

##### Create Iceberg Tables from Files Resources

In this code snippet two Iceberg tables are created from PySpark dataframes. The dataframes load CSV data from a CDE Files Resource specifying the ```/app/mount``` path.

```
# PySpark commands:
df1  = spark.read.csv("/app/mount/cell_towers_1.csv", header=True, inferSchema=True)
df1.writeTo("CELL_TOWERS_LEFT").using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()

df2  = spark.read.csv("/app/mount/cell_towers_2.csv", header=True, inferSchema=True)
df2.writeTo("CELL_TOWERS_RIGHT").using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()
```

##### Read Iceberg Tables using PySpark

Next, use Spark SQL to access the data from the Iceberg tables:

```
# Spark SQL Commands:
spark.sql("SELECT * FROM CELL_TOWERS_LEFT \
  WHERE manufacturer == 'TelecomWorld' \
  AND cell_tower_failure == 0").show()

# Expected Output:
+---+---------------+------------+------------------+----------+---------+------------+------------+------------+------------------+
| id|      device_id|manufacturer|        event_type| longitude| latitude|iot_signal_1|iot_signal_3|iot_signal_4|cell_tower_failure|
+---+---------------+------------+------------------+----------+---------+------------+------------+------------+------------------+
|  1|0x100000000001d|TelecomWorld|       battery 10%| -83.04828|51.610226|           9|          52|         103|                 0|
|  2|0x1000000000008|TelecomWorld|       battery 10%| -83.60245|51.892113|           6|          54|         103|                 0|
|  7|0x100000000000b|TelecomWorld|      device error| -83.62492|51.891964|           5|          54|         102|                 0|
| 12|0x1000000000020|TelecomWorld|system malfunction| -83.36766|51.873108|           8|          53|         106|                 0|
| 13|0x1000000000017|TelecomWorld|        battery 5%| -83.04949|51.906513|           4|          52|         105|                 0|
| 24|0x1000000000026|TelecomWorld|      device error| -83.15052|51.605473|           6|          55|         103|                 0|
| 30|0x1000000000008|TelecomWorld|       battery 10%| -83.44602| 51.60561|           2|          53|         106|                 0|
| 35|0x1000000000002|TelecomWorld|system malfunction| -83.62555|51.827686|           2|          54|         102|                 0|
| 37|0x100000000001d|TelecomWorld|       battery 10%| -83.47665|51.670994|           3|          53|         105|                 0|
| 41|0x1000000000017|TelecomWorld|      device error| -82.89744| 51.92945|           4|          52|         100|                 0|
+---+---------------+------------+------------------+----------+---------+------------+------------+------------+------------------+
```

##### Validate Iceberg Table

Use the ```SHOW TBLPROPERTIES``` command to validate Iceberg Table format:

```
# Spark SQL Command:
spark.sql("SHOW TBLPROPERTIES CELL_TOWERS_LEFT").show()

# Expected Output:
+--------------------+-------------------+
|                 key|              value|
+--------------------+-------------------+
| current-snapshot-id|8073060523561382284|
|              format|    iceberg/parquet|
|      format-version|                  1|
|write.format.default|            parquet|
+--------------------+-------------------+
```

As an alternative method to validate Iceberg Table format, investigate Iceberg Metadata with any of the following Spark SQL commands:

```
# Query Iceberg History Table
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_LEFT.history").show()
+--------------------+-------------------+---------+-------------------+
|     made_current_at|        snapshot_id|parent_id|is_current_ancestor|
+--------------------+-------------------+---------+-------------------+
|2024-10-08 20:30:...|8073060523561382284|     null|               true|
+--------------------+-------------------+---------+-------------------+

# Query Iceberg Partitions Table
+------------+----------+----------------------------+--------------------------+----------------------------+--------------------------+
|record_count|file_count|position_delete_record_count|position_delete_file_count|equality_delete_record_count|equality_delete_file_count|
+------------+----------+----------------------------+--------------------------+----------------------------+--------------------------+
|        1440|         1|                           0|                         0|                           0|                         0|
+------------+----------+----------------------------+--------------------------+----------------------------+--------------------------+

# Query Iceberg Snapshots Table
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_LEFT.snapshots").show()
+--------------------+-------------------+---------+---------+--------------------+--------------------+
|        committed_at|        snapshot_id|parent_id|operation|       manifest_list|             summary|
+--------------------+-------------------+---------+---------+--------------------+--------------------+
|2024-10-08 20:30:...|8073060523561382284|     null|   append|s3a://paul-aug26-...|{spark.app.id -> ...|
+--------------------+-------------------+---------+---------+--------------------+--------------------+

# Query Iceberg Refs Table
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_LEFT.refs").show()
+----+------+-------------------+-----------------------+---------------------+----------------------+
|name|  type|        snapshot_id|max_reference_age_in_ms|min_snapshots_to_keep|max_snapshot_age_in_ms|
+----+------+-------------------+-----------------------+---------------------+----------------------+
|main|BRANCH|8073060523561382284|                   null|                 null|                  null|
+----+------+-------------------+-----------------------+---------------------+----------------------+
```

##### Create Empty Iceberg Table using Spark SQL

You can also use Spark SQL to create an Iceberg Table.

Run a ```SHOW TABLE``` command on an existing table to investigate table format:

```
# Spark SQL Command:
print(spark.sql("SHOW CREATE TABLE CELL_TOWERS_LEFT").collect()[0][0])

# Expected Output
CREATE TABLE spark_catalog.default.cell_towers_left (
  `id` INT,
  `device_id` STRING,
  `manufacturer` STRING,
  `event_type` STRING,
  `longitude` DOUBLE,
  `latitude` DOUBLE,
  `iot_signal_1` INT,
  `iot_signal_3` INT,
  `iot_signal_4` INT,
  `cell_tower_failure` INT)
USING iceberg
LOCATION 's3a://paul-aug26-buk-a3c2b50a/data/warehouse/tablespace/external/hive/CELL_TOWERS_LEFT'
TBLPROPERTIES(
  'current-snapshot-id' = '8073060523561382284',
  'format' = 'iceberg/parquet',
  'format-version' = '1',
  'write.format.default' = 'parquet')
```

Next, create a new Iceberg table in the likes of this table. Notice the ```USING iceberg``` clause:

```
# Spark SQL Command:
spark.sql("""
CREATE TABLE ICE_TARGET_TABLE (
  `id` INT,
  `device_id` STRING,
  `manufacturer` STRING,
  `event_type` STRING,
  `longitude` DOUBLE,
  `latitude` DOUBLE,
  `iot_signal_1` INT,
  `iot_signal_3` INT,
  `iot_signal_4` INT,
  `cell_tower_failure` INT)
USING iceberg;
""")
```

This table is empty. Query Table Files to validate this:

```
# Spark SQL Command:
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.ICE_TARGET_TABLE.files;").show()

# Expected Output:
+-------+---------+-----------+-------+------------+------------------+------------+------------+-----------------+----------------+------------+------------+------------+-------------+------------+-------------+----------------+
|content|file_path|file_format|spec_id|record_count|file_size_in_bytes|column_sizes|value_counts|null_value_counts|nan_value_counts|lower_bounds|upper_bounds|key_metadata|split_offsets|equality_ids|sort_order_id|readable_metrics|
+-------+---------+-----------+-------+------------+------------------+------------+------------+-----------------+----------------+------------+------------+------------+-------------+------------+-------------+----------------+
+-------+---------+-----------+-------+------------+------------------+------------+------------+-----------------+----------------+------------+------------+------------+-------------+------------+-------------+----------------+
```

##### Append Data Into Empty Iceberg Table

Append data from a PySpark dataframe into an Iceberg table. Notice the use of the ```append()``` method.

```
# PySPark command:
df2.writeTo("SPARK_CATALOG.DEFAULT.ICE_TARGET_TABLE").using("iceberg").tableProperty("write.format.default", "parquet").append()
```

Query Iceberg Metadata in order to validate that the append operation completed successfully:

```
# Spark SQL Command:
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.ICE_TARGET_TABLE.files;").show()

# Expected Output:
+-------+--------------------+-----------+-------+------------+------------------+--------------------+--------------------+--------------------+----------------+--------------------+--------------------+------------+-------------+------------+-------------+--------------------+
|content|           file_path|file_format|spec_id|record_count|file_size_in_bytes|        column_sizes|        value_counts|   null_value_counts|nan_value_counts|        lower_bounds|        upper_bounds|key_metadata|split_offsets|equality_ids|sort_order_id|    readable_metrics|
+-------+--------------------+-----------+-------+------------+------------------+--------------------+--------------------+--------------------+----------------+--------------------+--------------------+------------+-------------+------------+-------------+--------------------+
|      0|s3a://paul-aug26-...|    PARQUET|      0|        1440|             36103|{1 -> 5796, 2 -> ...|{1 -> 1440, 2 -> ...|{1 -> 0, 2 -> 0, ...|{5 -> 0, 6 -> 0}|{1 -> , 2 -> ...|{1 -> �, 2 -> ...|        null|          [4]|        null|            0|{{286, 1440, 0, n...|
+-------+--------------------+-----------+-------+------------+------------------+--------------------+--------------------+--------------------+----------------+--------------------+--------------------+------------+-------------+------------+-------------+--------------------+
```

##### Create Iceberg Table from Hive Table

There are a few options to convert Hive tables into Iceberg Tables. The easiest approach is an "inplace-migration" to Iceberg table format.

Create a Hive Table using a PySpark dataframe:

```
# PySpark Command:
df1.write.mode("overwrite").saveAsTable('HIVE_TO_ICEBERG_TABLE', format="parquet")
```

Now migrate it to Iceberg table format:

```
spark.sql("ALTER TABLE HIVE_TO_ICEBERG_TABLE UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')")
spark.sql("CALL spark_catalog.system.migrate('HIVE_TO_ICEBERG_TABLE')")
```

Validate Icberg Table format:

```
# Spark SQL Command:
spark.sql("SHOW TBLPROPERTIES HIVE_TO_ICEBERG_TABLE").show()

# Expected Output:
+--------------------+--------------------+
|                 key|               value|
+--------------------+--------------------+
|   bucketing_version|                   2|
| current-snapshot-id| 1440783321004851162|
|external.table.purge|                TRUE|
|              format|     iceberg/parquet|
|      format-version|                   1|
|            migrated|                true|
|numFilesErasureCoded|                   0|
|schema.name-mappi...|[ {\n  "field-id"...|
+--------------------+--------------------+
```

## Summary

Cloudera Data Engineering (CDE) and the broader Cloudera Data Platform (CDP) offer a powerful, scalable solution for building, deploying, and managing data workflows in hybrid and multi-cloud environments. CDE simplifies data engineering with serverless architecture, auto-scaling Spark clusters, and built-in Apache Iceberg support.

Unlike competing offerings, each CDE release is certified against one or more Apache Iceberg versions. This ensures full compatibility between the Spark engine and the underlying Open Lakehouse capabilities, such as Apache Ranger for security policies. Whenever you launch a CDE Spark Job or Session, Iceberg dependencies are automatically configured as dictated by the chosen Spark version.

With full native Iceberg support, you can leverage CDE Sessions to create or migrate to Iceberg Table format without any special configurations.

## Next Steps

Here is a list of helpful articles and blogs related to Cloudera Data Engineering and Apache Iceberg:

- **Cloudera on Public Cloud 5-Day Free Trial**
   Experience Cloudera Data Engineering through common use cases that also introduce you to the platform’s fundamentals and key capabilities with predefined code samples and detailed step by step instructions.
   [Try Cloudera on Public Cloud for free](https://www.cloudera.com/products/cloudera-public-cloud-trial.html?utm_medium=sem&utm_source=google&keyplay=ALL&utm_campaign=FY25-Q2-GLOBAL-ME-PaidSearch-5-Day-Trial%20&cid=701Hr000001fVx4IAE&gad_source=1&gclid=EAIaIQobChMI4JnvtNHciAMVpAatBh2xRgugEAAYASAAEgLke_D_BwE)

- **Cloudera Blog: Supercharge Your Data Lakehouse with Apache Iceberg**  
   Learn how Apache Iceberg integrates with Cloudera Data Platform (CDP) to enable scalable and performant data lakehouse solutions, covering features like in-place table evolution and time travel.  
   [Read more on Cloudera Blog](https://blog.cloudera.com/supercharge-your-data-lakehouse-with-apache-iceberg-in-cloudera-data-platform/)

- **Cloudera Docs: Using Apache Iceberg in Cloudera Data Engineering**  
   This documentation explains how Apache Iceberg is utilized in Cloudera Data Engineering to handle massive datasets, with detailed steps on managing tables and virtual clusters.  
   [Read more in Cloudera Documentation](https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-using-iceberg.html)

- **Cloudera Blog: Building an Open Data Lakehouse Using Apache Iceberg**  
   This article covers how to build and optimize a data lakehouse architecture using Apache Iceberg in CDP, along with advanced features like partition evolution and time travel queries.  
   [Read more on Cloudera Blog](https://blog.cloudera.com/how-to-use-apache-iceberg-in-cdp-open-lakehouse/)
