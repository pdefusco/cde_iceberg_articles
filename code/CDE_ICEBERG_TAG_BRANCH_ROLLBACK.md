# New Features in Apache Iceberg 1.3

CDP Data Engineering (CDE) is the only cloud-native service purpose-built for enterprise data engineering teams. Building on Apache Spark, Data Engineering is an all-inclusive data engineering toolset that enables orchestration automation with Apache Airflow, advanced pipeline monitoring, visual troubleshooting, and comprehensive management tools to streamline ETL processes across enterprise analytics teams.

Apache Iceberg is an open table format format for huge analytic datasets. It provides features that, coupled with Spark as the compute engine, allows you to build data processing pipelines with dramatic gains in terms of scalability, performance, and overall developer productivity.

CDE Provides native Iceberg support. With the release of CDE 1.20 the Spark Runtime has been updated with Apache Iceberg 1.3. This version introduces new features that provide great benefits to Data Engineers.

1. Table Branching: ability to create independent lineages of snapshots, each with its own lifecycle.
2. Table Tagging: ability to tag an Iceberg table snapshot.
3. Table rollbacks: ability to technique reverse or “roll back” table to a previous state.

## Requirements

* CDE Virtual Cluster of type "All-Purpose" running in CDE Service with version 1.19 or above.
* A working installation of the CDE CLI.

## Step by Step Instructions

### Requirements

* A CDE Service in Private or Public Cloud on version 1.21 or above.

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

df  = spark.read.csv("/app/mount/cell_towers_1.csv", header=True, inferSchema=True)
df.writeTo("CELL_TOWERS_{}".format(USERNAME)).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()
```

### Working with Iceberg Table Branches

##### Create Iceberg Table Branch

```
# CREATE TABLE BRANCH - Skip: Not supported
spark.sql("ALTER TABLE CELL_TOWERS_{} \
  CREATE BRANCH ingestion_branch \
  RETAIN 7 DAYS \
  WITH RETENTION 2 SNAPSHOTS;".format(USERNAME))

# SET TABLE BRANCH AS ACTIVE - Skip: Not supported
spark.sql("SET spark.wap.branch = 'ingestion_branch';")
```

##### Upsert Data into Branch with Iceberg Merge Into

```
# LOAD NEW TRANSACTION BATCH
batchDf = spark.read.csv("/app/mount/cell_towers_2.csv", header=True, inferSchema=True)
batchDf.printSchema()
batchDf.createOrReplaceTempView("BATCH_TEMP_VIEW".format(USERNAME))

# CREATE TABLE BRANCH - Supported
spark.sql("ALTER TABLE CELL_TOWERS_{} CREATE BRANCH ingestion_branch".format(USERNAME))
# WRITE DATA OPERATION ON TABLE BRANCH - Supported
batchDf.write.format("iceberg").option("branch", "ingestion_branch").mode("append").save("CELL_TOWERS_{}".format(USERNAME))
```

Notice that a simple SELECT query against the table still returns the original data.

```
spark.sql("SELECT * FROM CELL_TOWERS_{};".format(USERNAME)).show()
```

If you want to access the data in the branch, you can specify the branch name in your SELECT query.

```
spark.sql("SELECT * FROM CELL_TOWERS_{} VERSION AS OF 'ingestion_branch';".format(USERNAME)).show()
```

We can use the "fast forward" routine to incorporate validated changes from the new branch.

```
table
  .manageSnapshots()
  .fastForward("main", "ingestion_branch")
  .commit();
```

Track table snapshots post Merge Into operation:

```
# QUERY ICEBERG METADATA HISTORY TABLE
spark.sql("SELECT * FROM CELL_TOWERS_{}.snapshots".format(USERNAME)).show(20, False)
```

### Cherrypicking Snapshots

The cherrypick_snapshot procedure creates a new snapshot incorporating the changes from another snapshot in a metadata-only operation (no new datafiles are created). To run the cherrypick_snapshot procedure you need to provide two parameters: the name of the table you’re updating as well as the ID of the snapshot the table should be updated based on. This transaction will return the snapshot IDs before and after the cherry-pick operation as source_snapshot_id and current_snapshot_id.

we will use the cherrypick operation to commit the changes to the table which were staged in the 'ingestion_branch' branch up until now.

```
# SHOW PAST BRANCH SNAPSHOT ID'S
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_{}.refs;".format(USERNAME)).show()

# SAVE THE SNAPSHOT ID CORRESPONDING TO THE CREATED BRANCH
branchSnapshotId = spark.sql("SELECT snapshot_id FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_{}.refs WHERE NAME == 'ingestion_branch';".format(USERNAME)).collect()[0][0]

# USE THE PROCEDURE TO CHERRY-PICK THE SNAPSHOT
# THIS IMPLICITLY SETS THE CURRENT TABLE STATE TO THE STATE DEFINED BY THE CHOSEN PRIOR SNAPSHOT ID
spark.sql("CALL spark_catalog.system.cherrypick_snapshot('SPARK_CATALOG.DEFAULT.CELL_TOWERS_{0}',{1})".format(USERNAME, branchSnapshotId))

# VALIDATE THE CHANGES
# THE TABLE ROW COUNT IN THE CURRENT TABLE STATE REFLECTS THE APPEND OPERATION - IT PREVIOSULY ONLY DID BY SELECTING THE BRANCH
spark.sql("SELECT COUNT(*) FROM CELL_TOWERS_{};".format(USERNAME)).show()
```


### Working with Iceberg Table Tags

##### Create Table Tag

Tags are immutable labels for Iceberg Snapshot ID's and can be used to reference a particular version of the table via a simple tag rather than having to work with Snapshot ID's directly.   

```
spark.sql("ALTER TABLE SPARK_CATALOG.DEFAULT.CELL_TOWERS_{} CREATE TAG businessOrg RETAIN 365 DAYS".format(USERNAME)).show()
```

Select your table snapshot as of a particular tag:

```
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_{} VERSION AS OF 'businessOrg';".format(USERNAME)).show()
```


### Table Rollbacks

##### Rollback Table to previous Snapshot

Rollback table to state prior to merge into.

```
# SHOW LASTEST TABLE HISTORY
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_{}.HISTORY;".format(USERNAME)).show()

# SELECT SECOND LAST SNAPSHOT ID FROM HISTORY
priorSnapshotId = spark.sql("SELECT MIN(SNAPSHOT_ID) FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_{}.HISTORY".format(USERNAME)).collect()[0][0]

# REPLACE ICEBERG SNAPSHOT ID WITH VALUE OBTAINED ABOVE
spark.sql("CALL SPARK_CATALOG.rollback_to_snapshot('DEFAULT.CELL_TOWERS_{0}', {1})".format(USERNAME, priorSnapshotId))

# VALIDATE ROLLBACK BY VERIFYING TABLE COUNT
spark.sql("SELECT COUNT(*) FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_{}".format(USERNAME)).show()
```

### The refs Metadata Table

The refs metadata table helps you understand and manage your table’s snapshot history and retention policy, making it a crucial part of maintaining data versioning and ensuring that your table’s size is under control. Among its many use cases, the table provides a list of all the named references within an Iceberg table sich as Branch names and corresponding Snapshot ID's.

```
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_{}.refs;".format(USERNAME)).show()
```


## Summary & Next Steps

CDE supports Apache Iceberg which provides a table format for huge analytic datasets in the cloud. Iceberg enables you to work with large tables, especially on object stores, and supports concurrent reads and writes on all storage media. You can use Cloudera Data Engineering virtual clusters running Spark 3 to interact with Apache Iceberg tables.

The Iceberg Metadata Layer can track snapshots under different paths or give particular snapshots a name. These features are respectively called table branching and tagging. Thanks to them, Iceberg Data Engineers can implement data pipelines with advanced isolation, reproducibility, and experimentation capabilities.

Table-level rollbacks are a specific Iceberg feature that allows changes to a table to be reversed to a previous state. This capability provides extra-safety in a variety of scenarios during the implementation of new use cases in Production.
