# ICEBERG MERGE INTO with Spark in Cloudera Data Engineering

## Objective

This article provides an introduction to the Iceberg Merge Into using Spark SQL in Cloudera Data Engineering (CDE). CDE provides native Apache Iceberg Table Format support in its Spark Runtimes. Iceberg supports the Merge Into command, a great alternative to Spark upserts which will save you time and effort when running ETL jobs at scale.

## Abstract

CDP Data Engineering (CDE) is the only cloud-native service purpose-built for enterprise data engineering teams. Building on Apache Spark, Data Engineering is an all-inclusive data engineering toolset that enables orchestration automation with Apache Airflow, advanced pipeline monitoring, visual troubleshooting, and comprehensive management tools to streamline ETL processes across enterprise analytics teams.

Apache Iceberg is an open table format format for huge analytic datasets. It provides features that, coupled with Spark as the compute engine, allows you to build data processing pipelines with dramatic gains in terms of scalability, performance, and overall developer productivity.

The Merge Into Iceberg command in particular allows for upserting (updating or inserting) data into a target table based on a source dataset. It follows the SQL standard for merging data, enabling users to perform complex updates and inserts in a single operation.

#### Key Features:
- **Conditional Updates**: Rows in the target table can be updated if they match certain conditions in the source.
- **Insert New Records**: Rows in the source that don’t have a match in the target can be inserted.
- **Deletes**: Conditional deletes can also be executed during a merge operation.

#### Syntax Example:
```sql
MERGE INTO target_table AS target
USING source_table AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET target.col1 = source.col1
WHEN NOT MATCHED THEN INSERT (id, col1) VALUES (source.id, source.col1)
```

The `MERGE INTO` command is efficient because Iceberg maintains snapshot-based, immutable table structures, which optimize for performance while ensuring consistency and correctness.

## Requirements

* CDE Virtual Cluster of type "All-Purpose" running in CDE Service with version 1.19 or above, and Spark version 3.2 or above.
* A working installation of the CDE CLI.

## Step by Step Instructions

#### Create CDE Files Resource

```
cde resource create --name myFiles --type files
cde resource upload --name myFiles --local-path resources/cell_towers_1.csv --local-path resources/cell_towers_2.csv
```

#### Launch CDE Session & Run Spark Commands

```
cde session create --name icebergSession --type pyspark --mount-1-resource myFiles
cde session interact --name icebergSession
```

##### Create Iceberg Table

```
USERNAME = "pauldefusco"

df1  = spark.read.csv("/app/mount/cell_towers_1.csv", header=True, inferSchema=True)
df1.writeTo("CELL_TOWERS_LEFT_{}".format(USERNAME)).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()

df2  = spark.read.csv("/app/mount/cell_towers_2.csv", header=True, inferSchema=True)
df2.writeTo("CELL_TOWERS_RIGHT_{}".format(USERNAME)).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()
```

##### Iceberg Merge Into

```
# PRE-MERGE COUNTS BY TRANSACTION TYPE:
spark.sql("""SELECT COUNT(id) FROM CELL_TOWERS_LEFT_{} GROUP BY event_type""".format(username)).show()

# MERGE OPERATION
spark.sql("""MERGE INTO CELL_TOWERS_LEFT_{} t   
USING (SELECT * FROM CELL_TOWERS_RIGHT_{}) s          
ON t.id = s.id               
WHEN MATCHED AND t.longitude < -70 AND t.latitude > 50 AND t.manufacturer == "TelecomWorld" THEN UPDATE SET t.event_type = "invalid"
WHEN NOT MATCHED THEN INSERT *""".format(username))

# POST-MERGE COUNTS:
spark.sql("""SELECT COUNT(id) FROM CELL_TOWERS_LEFT_{} GROUP BY EVENT_TYPE""".format(username)).show()
```

## Summary

Cloudera Data Engineering (CDE) and the broader Cloudera Data Platform (CDP) offer a powerful, scalable solution for building, deploying, and managing data workflows in hybrid and multi-cloud environments. CDE simplifies data engineering with serverless architecture, auto-scaling Spark clusters, and built-in Apache Iceberg support. Combined with CDP’s enterprise-grade security, governance, and flexibility across public and private clouds, these platforms enable organizations to efficiently process large-scale data while maintaining compliance, agility, and control over their data ecosystems.

## Next Steps

Here is a list of helpful articles and blogs related to Cloudera Data Engineering and Apache Iceberg:

- **Cloudera Blog: Supercharge Your Data Lakehouse with Apache Iceberg**  
   Learn how Apache Iceberg integrates with Cloudera Data Platform (CDP) to enable scalable and performant data lakehouse solutions, covering features like in-place table evolution and time travel.  
   [Read more on Cloudera Blog](https://blog.cloudera.com/supercharge-your-data-lakehouse-with-apache-iceberg-in-cloudera-data-platform/)

- **Cloudera Docs: Using Apache Iceberg in Cloudera Data Engineering**  
   This documentation explains how Apache Iceberg is utilized in Cloudera Data Engineering to handle massive datasets, with detailed steps on managing tables and virtual clusters.  
   [Read more in Cloudera Documentation](https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-using-iceberg.html)

- **Cloudera Blog: Building an Open Data Lakehouse Using Apache Iceberg**  
   This article covers how to build and optimize a data lakehouse architecture using Apache Iceberg in CDP, along with advanced features like partition evolution and time travel queries.  
   [Read more on Cloudera Blog](https://blog.cloudera.com/how-to-use-apache-iceberg-in-cdp-open-lakehouse/)

These resources will give you a deep dive into how Cloudera Data Engineering leverages Apache Iceberg for scalable, efficient data management.
