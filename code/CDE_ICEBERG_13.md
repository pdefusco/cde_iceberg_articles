# New Features in Apache Iceberg 1.3

With the release of CDE 1.20 the CDE Runtime has been updated with Apache Iceberg 1.3. This version introduces three features that provide great benefits to Data Engineers.

1. Table Branching: ability to create independent lineages of snapshots, each with its own lifecycle.
2. Table Tagging: ability to tag an Iceberg table snapshot.
3. Table rollbacks: ability to technique reverse or “roll back” table to a previous state.

## Requirements

* CDE Virtual Cluster of type "All-Purpose" running in CDE Service with version 1.19 or above.
* A working installation of the CDE CLI.

## Step by Step Instructions

### Prerequisites

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


### Working with Iceberg Table Tags

##### Create Table Tag

```
spark.sql("ALTER TABLE CELL_TOWERS_{}
  CREATE TAG 'business-unit-tag'
  AS OF VERSION 8
  RETAIN 14 DAYS;".format(USERNAME))
```

Select your table snapshot as of a particular tag:

```
spark.sql("SELECT * FROM prod.db.table VERSION AS OF 'historical-snapshot';").show()
```

##### Rollback Table to previous Tag

Rollback table to state prior to merge into.

```
# REPLACE ICEBERG SNAPSHOT ID WITH VALUE OBTAINED ABOVE
icebergSnapshotId = ""
spark.sql("CALL catalog.database.rollback_to_snapshot('orders', 12345)")
```


## Summary & Next Steps

CDE supports Apache Iceberg which provides a table format for huge analytic datasets in the cloud. Iceberg enables you to work with large tables, especially on object stores, and supports concurrent reads and writes on all storage media. You can use Cloudera Data Engineering virtual clusters running Spark 3 to interact with Apache Iceberg tables.

The Iceberg Metadata Layer can track snapshots under different paths or give particular snapshots a name. These features are respectively called table branching and tagging. Thanks to them, Iceberg Data Engineers can implement data pipelines with advanced isolation, reproducibility, and experimentation capabilities.

Table-level rollbacks are a specific Iceberg feature that allows changes to a table to be reversed to a previous state. This capability provides extra-safety in a variety of scenarios during the implementation of new use cases in Production.
