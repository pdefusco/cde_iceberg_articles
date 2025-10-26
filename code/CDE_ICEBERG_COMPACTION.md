# ICEBERG COMPACTION with Spark in Cloudera Data Engineering

## Objective

This article provides an introduction to Iceberg Table Compaction using Spark SQL in Cloudera Data Engineering (CDE). Compaction is the process of merging many small data files into fewer, larger ones to improve query performance and reduce metadata overhead. Over time, frequent inserts or streaming writes can fragment tables into thousands of small files and lead to slow reads.

CDE Spark Data Engineers typically schedule compaction as a maintenance task using Iceberg’s built-in SQL procedures in order to keep data in optimal file sizes, ensuring efficient analytics and stable job performance.

In this tutorial you will create a CDE Session and interact with Apache Iceberg tables in oreder to run compaction using PySpark.

## Requirements

* CDE Virtual Cluster of type "All-Purpose" running in CDE Service with version 1.23 or above, and Spark version 3.5 or above.
* An installation of the CDE CLI is recommended but optional. In the steps below you will create the CDE Session using the CLI, but you can alternatively launch one using the UI, or run the same commands via Spark Connect.

## Step by Step Instructions

### Create Unevenly Distributed Data

Create CDE Depdendencies:

```
cde resource create \
  --name datagen-env

cde resource upload \
  --name datagen-env \
  --local-path compaction_resources/requirements.txt

cde resource create \
  --name files-spark35 \
  --type files

cde resource upload \
  --name files-spark35 \
  --local-path compaction_resources/datagen.py
```

Run DataGen Job:

```
cde job delete \
  --name datagen_10M

cde job create \
  --name datagen_10M \
  --arg 25 \
  --arg 25 \
  --arg 10000000 \
  --arg db_10M \
  --arg source_10M \
  --type spark \
  --mount-1-resource files-spark32 \
  --application-file datagen.py \
  --runtime-image-resource-name datagen-runtime \
  --executor-cores 4 \
  --executor-memory "4g"

cde job run \
  --name datagen_10M \
  --executor-memory "10g"
```

Next, we will use the data generated in order to create four Iceberg tables through a CDE Session. All tables will have the same data, but they will be partitioned differently according to different partitioning and bucketing schemes.

First, launch the CDE Session:

```
cde session create \
  --name spark_bucketing_test \
  --type pyspark \
  --executor-cores 5 \
  --executor-memory "10g" \
  --num-executors 5 \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.maxExecutors=20
```

Next, in the CDE Session, validate table creation.

```
# Session Commands:

db_name = "db_10M"
src_tbl = "source_10M"
tgt_tbl = "target_10M"

spark.sql("drop table if exists {0}.{1}".format(db_name, tgt_tbl))
df = spark.sql("select * from {0}.{1}".format(db_name, src_tbl))

# Spark SQL Command:
print(spark.sql("SHOW CREATE TABLE {0}.{1}".format(db_name, src_tbl)).collect()[0][0])

# Expected Output:
CREATE TABLE `db_10M`.`source_10M` (
  `unique_id` INT,
  `code` STRING,
  `col1` FLOAT,
  `col2` FLOAT,
  .
  .
  .
  `col99` FLOAT)
USING parquet
LOCATION 's3a://xxx/data/warehouse/tablespace/external/hive/db_10m.db/source_10m'
TBLPROPERTIES (
  'numFilesErasureCoded' = '0',
  'bucketing_version' = '2',
  'TRANSLATED_TO_EXTERNAL' = 'TRUE',
  'external.table.purge' = 'TRUE')
```

##### Example 1: Repartition the source table into a single partition, then bucket and sort by "unique_id"

This produced 25 evenly sized files of 64 MB each corresponding to 25 buckets. Each row is assigned into a bucket by means of a hashing function. On disk, the data will be written into files named "part-0000" but with 25 separate bucket ID's.

```
## PySpark Code in Session:

db_name = "db_10M"
src_tbl = "source_10M"
tgt_tbl = "ex_1"
buckets = 25

df.repartition(1) \
  .write \
  .mode("overwrite") \
  .bucketBy(25, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + tgt_tbl)
```

##### Example 2: Repartition the source table into 25 partitions, then bucket and sort by "unique_id".

* This produces 25 partitions, where within each partition each row is assigned to a bucket by means of a hashing function.
* Notice we are not asking spark to repartition by a specific column. Spark just uses internal statistics to produce an as even as possible distribution of data among partitions.
* Depending on how data is distributed among each of the 25 partitions i.e. how skewed it is, Spark will hash each row based on "unique_id" and create bucket files within each partition.
* The max number of possible files created is 625 for perfectly evenly distributed data. In this case, each file is roughly 6MB.

```
## PySpark Code in Session:

db_name = "db_10M"
src_tbl = "source_10M"
tgt_tbl = "ex_2"
buckets = 25

df.repartition(25) \
  .write \
  .mode("overwrite") \
  .bucketBy(25, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + tgt_tbl)
```

##### Example 3: Repartition the source table based on a column with low cardinality and even distribution (i.e. low skew) and then bucket and sort with "unique_id".

* Generally the best approache among the examples so far, especially at larger scale. However, it requires identifying a column that has relatively low cardinality and even distribution.
* In this case this is "code"  which uniformly takes a value among a list of 9 string codes. But I know this because I generate the synthetic data. In your case you'd have to do some tests with your data.
This creates a maximum of 9 partitions x 25 buckets (225), reflecting the number of unique code categories and requested buckets.
* Notice ther heavy data skew between the different values.

Explore cardinality of the "Code" column:

```
## PySpark Code in Session:

db_name = "db_10M"
src_tbl = "source_10M"
tgt_tbl = "ex_3"
buckets = 25

spark.sql("""SELECT CODE, COUNT(*)
            FROM {0}.{1}
            GROUP BY CODE""".format(db_name, src_tbl)).show()

## Expected Output:
+----+--------+
|CODE|count(1)|
+----+--------+
|   g| 1819875|
|   f| 1817273|
|   e|  909405|
|   h| 1363761|
|   d|  911224|
|   c|  909449|
|   i|  907558|
|   b|  907035|
|   a|  454420|
+----+--------+
```

Now bucket your data by "unique_id", but repartition by "code":

```
df.repartition("code") \
  .write \
  .mode("overwrite") \
  .bucketBy(buckets, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + tgt_tbl)
```

##### Example 4: Repartition by evenly distributed column with low cardinality, but reduce number of partitions even further

* Because the number of files can reach the product of the number of partitions and the number of buckets, "bucketBy" can be very slow for large datasets where partitions are randomly arranged with respect to bucketing columns.
* Repartitioning can be dramatically faster when the number of partitions is set equal to the number of buckets, and the repartitioning key is made up of the bucketing columns. In other words, you will get the lowest number of files when you can align buckets and partitions i.e. having the repartitoning key made up of bucketing column(s).
* However, this is hard to achieve because the data may not lend itself to this. Regardless, testing different combinations of repartitioning and bucketing keys with the objective of getting to an as small as possible number of files, aiming at roughly 128 MB filesize, is an exercise that goes in the direction of this "ideal" scenario.  
* In the below example, the data is repartitioned by the low cardinality "code" column, as before, but this time the number of partitions is reduced to 5 resulting in 125 files i.e. a lower number of files each with more data than the previous example 3. Notice that the data skew in the "code" column leads to file size differences, with many files reaching ~40MB while others ~20MB or even as low as 7 MB in size.

```
# PySpark Code

db_name = "db_10M"
src_tbl = "source_10M"
tgt_tbl = "ex_4"
buckets = 25

df.repartition(5, "code") \
  .write \
  .mode("overwrite") \
  .bucketBy(buckets, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name+"." + tgt_tbl)
```

### Migrate Tables from Hive to Iceberg

Run the following commands in order to migrate the four tables to Iceberg without losing the Hive Table Physical Layout.

First, show location of each of the four Hive tables:

```
# List of table names you want to check
tables = [
    "db_10M.ex_1",
    "db_10M.ex_2",
    "db_10M.ex_3",
    "db_10M.ex_4"
]

# Loop through each table and run the query
for tbl in tables:
    print(f"\n=== Table: {tbl} ===")
    query = f"""
        SHOW CREATE TABLE {tbl}
    """
    proint("Hive Table Location:")
    print(spark.sql(query).collect()[0][0]))
    LOCATION = spark.sql(query).collect()[0][0])
    ICE_TABLE_NAME = tbl.split(".")[1]
    ICE_TABLE_NAME = "SPARK_CATALOG.DEFAULT." + ICE_TABLE_NAME
    print(f"\N=== Migrating to Iceberg Table: {ICE_TABLE_NAME} ===")

    # Migrate Table to Iceberg:
    spark.sql("""
      CREATE TABLE {}
      USING iceberg
      LOCATION '{}'
    """.format(ICE_TABLE_NAME, LOCATION))
```

### Look for Compaction Candidates

Now that we have four tables, which one is best suited for compaction?

Let's answer that by investigating how many files are found per partition, for each table?

```
# List of table names you want to check
tables = [
    "spark_catalog.default.ex_1",
    "spark_catalog.default.ex_2",
    "spark_catalog.default.ex_3",
    "spark_catalog.default.ex_4"
]

# Loop through each table and run the query
for tbl in tables:
    print(f"\n=== Table: {tbl} ===")
    query = f"""
        SELECT PARTITION, FILE_COUNT
        FROM {tbl}.partitions
    """
    spark.sql(query).show(truncate=False)
```

What is the size of each partition, for each table?

```
# Loop through each table and run the query
for tbl in tables:
    print(f"\n=== Table: {tbl} ===")
    query = f"""
        SELECT PARTITION, SUM(file_size_in_bytes) AS PARTITION_SIZE
        FROM {tbl}.FILES GROUP BY PARTITION
    """
    spark.sql(query).show(truncate=False)
```

#### Run Compaction

Now that we identified the table we must run compaction on, we can finally do so with the following call procedure:

```
from pyspark.sql import SparkSession

table_identifier = "<table_name_here>"

# Use SQL to call the procedure
spark.sql(f"""
  CALL my_catalog.system.rewrite_data_files(
    table => '{table_identifier}',
    strategy => 'binpack'
  )
""")
```

Now validate new file count per partition:

```
spark.sql("""
  SELECT PARTITION, FILE_COUNT FROM SPARK_CATALOG.TABLE.PARTITIONS
""").show()
```

## Summary

Cloudera Data Engineering (CDE) and the broader Cloudera Data Platform (CDP) offer a powerful, scalable solution for building, deploying, and managing data workflows in hybrid and multi-cloud environments. CDE simplifies data engineering with serverless architecture, auto-scaling Spark clusters, and built-in Apache Iceberg support. Combined with CDP’s enterprise-grade security, governance, and flexibility across public and private clouds, these platforms enable organizations to efficiently process large-scale data while maintaining compliance, agility, and control over their data ecosystems.

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
