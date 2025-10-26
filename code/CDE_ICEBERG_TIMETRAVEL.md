# ICEBERG TABLE WRITE and READ with Spark in Cloudera Data Engineering

## Objective

This article provides an introduction to the Iceberg using Spark SQL in Cloudera Data Engineering (CDE). CDE provides native Apache Iceberg Table Format support in its Spark Runtimes. This means you can create and interact with Iceberg Table format tables without any configurations.

## Abstract

CDP Data Engineering (CDE) is the only cloud-native service purpose-built for enterprise data engineering teams. Building on Apache Spark, Data Engineering is an all-inclusive data engineering toolset that enables orchestration automation with Apache Airflow, advanced pipeline monitoring, visual troubleshooting, and comprehensive management tools to streamline ETL processes across enterprise analytics teams.

Apache Iceberg is an open table format format for huge analytic datasets. It provides features that, coupled with Spark as the compute engine, allows you to build data processing pipelines with dramatic gains in terms of scalability, performance, and overall developer productivity.

Iceberg is natively supported by CDE. Any time a CDE Spark Job or Session is created, Iceberg dependencies are automatically set in the SparkSession without any need for configurations. As a CDP User, the CDE Data Engineer can thus create, read, modify, and interact with Iceberg tables as allowed by Ranger policies, whether these were created in Cloudera Data Warehouse (CDW), DataHub, or Cloudera AI (CML).

In this tutorial you will create a CDE Session and interact with Apache Iceberg tables using PySpark.

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
    --local-path resources/cell_towers_2.csv
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

df2  = spark.read.csv("/app/mount/cell_towers_2.csv", header=True, inferSchema=True)
df2.writeTo("CELL_TOWERS_RIGHT_{}".format(USERNAME)).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()
```

##### Iceberg Merge Into

```
# PRE-MERGE COUNTS BY TRANSACTION TYPE:
spark.sql("""SELECT COUNT(id) FROM CELL_TOWERS_LEFT_{} GROUP BY event_type""".format(USERNAME)).show()

# MERGE OPERATION
spark.sql("""MERGE INTO CELL_TOWERS_LEFT_{0} t   
USING (SELECT * FROM CELL_TOWERS_RIGHT_{0}) s          
ON t.id = s.id               
WHEN MATCHED AND t.longitude < -70 AND t.latitude > 50 AND t.manufacturer == "TelecomWorld" THEN UPDATE SET t.event_type = "invalid"
WHEN NOT MATCHED THEN INSERT *""".format(USERNAME))

# POST-MERGE COUNTS:
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
#second_snapshot = snapshots_df.select("snapshot_id").collect()[1][0]
first_snapshot = snapshots_df.select("snapshot_id").head(1)[0][0]

incReadDf = spark.read\
    .format("iceberg")\
    .option("start-snapshot-id", first_snapshot)\
    .option("end-snapshot-id", last_snapshot)\
    .load("SPARK_CATALOG.DEFAULT.CELL_TOWERS_LEFT_{}.snapshots".format(USERNAME))

print("Incremental Report:")
incReadDf.show()
```

##### Drop Tables

```
spark.sql("DROP TABLE SPARK_CATALOG.DEFAULT.CELL_TOWERS_LEFT_{} PURGE;".format(USERNAME))
spark.sql("DROP TABLE SPARK_CATALOG.DEFAULT.CELL_TOWERS_RIGHT_{} PURGE;".format(USERNAME))
```
