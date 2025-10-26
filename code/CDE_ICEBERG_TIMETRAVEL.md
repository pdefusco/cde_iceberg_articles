# ICEBERG TABLE WRITE and READ with Spark in Cloudera Data Engineering

## Objective

This article provides an introduction to Iceberg Time Travel using Spark SQL in Cloudera Data Engineering (CDE). CDE provides native Apache Iceberg Table Format support in its Spark Runtimes. This means you can create and interact with Iceberg Table format tables without any configurations.

Iceberg time travel is a feature that allows data engineers to query historical snapshots of a table at a specific point in time or by snapshot ID. Unlike traditional Hive tables, where changes overwrite data in place, Iceberg maintains a versioned history of all changes, including inserts, updates, and deletes.

CDE Spark Data Engineers use Iceberg Time Travel to reproduce past reports, debug issues, or recover accidentally deleted data by simply specifying the desired timestamp or snapshot when reading the table in PySpark.

## Requirements

* CDE Virtual Cluster of type "All-Purpose" running in CDE Service with version 1.22 or above, and Spark version 3.2 or above.
* An installation of the CDE CLI is recommended but optional. In the steps below you will create the CDE Session using the CLI, but you can alternatively launch one using the UI.

## Step by Step Instructions

#### Create CDE Files Resource

```
cde resource create \
    --name myFiles \
    --type files

cde resource upload \
    --name myFiles \
    --local-path resources/cell_towers_1.csv \
    --local-path resources/cell_towers_3.csv
```

#### Launch CDE Session & Run Spark Commands

```
cde session create \
    --name icebergsession \
    --type pyspark \
    --mount-1-resource myFiles

cde session interact \
    --name icebergsession
```

##### Create Iceberg Table

```
USERNAME = "pauldefusco"

df1  = spark.read.csv("/app/mount/cell_towers_1.csv", header=True, inferSchema=True)
df1.writeTo("CELL_TOWERS_LEFT_{}".format(USERNAME)).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()

df2  = spark.read.csv("/app/mount/cell_towers_3.csv", header=True, inferSchema=True)
df2.writeTo("CELL_TOWERS_RIGHT_{}".format(USERNAME)).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()
```

##### Iceberg Append

```
# PRE-APPEND COUNTS BY EVENT TYPE:
spark.sql("""SELECT COUNT(id) FROM CELL_TOWERS_LEFT_{} GROUP BY EVENT_TYPE""".format(USERNAME)).show()

# APPEND OPERATION WITH SPARK SQL
spark.sql("""
    INSERT INTO SPARK_CATALOG.DEFAULT.CELL_TOWERS_LEFT_{0}
    SELECT * FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_RIGHT_{0}
""".format(USERNAME))

# POST-APPEND COUNTS BY EVENT TYPE:
spark.sql("""SELECT COUNT(id) FROM CELL_TOWERS_LEFT_{} GROUP BY EVENT_TYPE""".format(USERNAME)).show()
```

##### Iceberg Time Travel

```
# ICEBERG TABLE SNAPSHOTS (USEFUL FOR INCREMENTAL QUERIES AND TIME TRAVEL)
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_LEFT_{}.snapshots".format(USERNAME)).show()

# STORE FIRST AND LAST SNAPSHOT ID'S FROM SNAPSHOTS TABLE
snapshots_df = spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_LEFT_{}.snapshots;".format(USERNAME))

# SNAPSHOTS
snapshots_df.show()
```

```
last_snapshot = snapshots_df.select("snapshot_id").tail(1)[0][0]
first_snapshot = snapshots_df.select("snapshot_id").head(1)[0][0]

incReadDf = spark.read\
    .format("iceberg")\
    .option("start-snapshot-id", first_snapshot)\
    .option("end-snapshot-id", last_snapshot)\
    .load("SPARK_CATALOG.DEFAULT.CELL_TOWERS_LEFT_{}".format(USERNAME))

print("Incremental Report:")
incReadDf.show()
print("Incremental Read:")
from pyspark.sql import functions as F
incReadDf.groupBy("event_type").agg(F.count("*").alias("row_count")).show()
```

The output of the incremental read reflects the rows that have changed between the two snapshots. Because the only rows affected were the rows appended, the output of the incremental read is identical to the data contained in the CELL_TOWERS_RIGHT table.

##### Drop Tables

```
spark.sql("DROP TABLE SPARK_CATALOG.DEFAULT.CELL_TOWERS_LEFT_{} PURGE;".format(USERNAME))
spark.sql("DROP TABLE SPARK_CATALOG.DEFAULT.CELL_TOWERS_RIGHT_{} PURGE;".format(USERNAME))
```

## Summary

In this tutorial you learned how to run an Incremental Read with Iceberg Time Travel. In this specific case, the Incremental Read allowed you to query an Iceberg Table for the rows that changed in between two operations. In general, Time travel is particularly valuable in data engineering workflows for auditing, reproducibility, and safe experimentation on evolving datasets.

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
