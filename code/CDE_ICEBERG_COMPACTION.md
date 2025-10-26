# ICEBERG COMPACTION with Spark in Cloudera Data Engineering

## Objective

This article provides an introduction to Iceberg Table Compaction using Spark SQL in Cloudera Data Engineering (CDE). Compaction is the process of merging many small data files into fewer, larger ones to improve query performance and reduce metadata overhead. Over time, frequent inserts or streaming writes can fragment tables into thousands of small files and lead to slow reads.

CDE Spark Data Engineers typically schedule compaction as a maintenance task using Iceberg’s built-in SQL procedures in order to keep data in optimal file sizes, ensuring efficient analytics and stable job performance.

In this tutorial you will create a CDE Session and interact with Apache Iceberg tables in oreder to run compaction using PySpark.

## Requirements

* CDE Virtual Cluster of type "All-Purpose" running in CDE Service with version 1.23 or above, and Spark version 3.5 or above.
* An installation of the CDE CLI is recommended but optional. In the steps below you will create the CDE Session using the CLI, but you can alternatively launch one using the UI, or run the same commands via Spark Connect.

## Step by Step Instructions

#### Create Unevenly Distributed Data

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
  --local-path jobs/datagen.py
```

#### Look for Compaction Candidates

How many files per partition?

```
spark.sql("""
  SELECT PARTITION, FILE_COUNT FROM SPARK_CATALOG.TABLE.PARTITIONS
""").show()
```

What is the size of each partition?

```
spark.sql("""
  SELECT PARTITION, SUM(file_size_in_bytes) AS PARTITION_SIZE FROM SPARK_CATALOG.CELL_TOWERS.FILES GROUP BY PARTITION
""").show()
```

#### Run Compaction

```
from pyspark.sql import SparkSession

table_identifier = "SPARK_CATALOG.DEFAULT.CELL_TOWERS"

# Use SQL to call the procedure (simplest)
spark.sql(f"""
  CALL my_catalog.system.rewrite_data_files(
    table => '{table_identifier}',
    strategy => 'binpack'   -- or 'sort'
  )
""")
```

Now validate new file count per partition:

```
spark.sql("""
  SELECT PARTITION, FILE_COUNT FROM SPARK_CATALOG.TABLE.PARTITIONS
""").show()
```
