# Monitoring Tables with Iceberg Metadata

This article is WIP.


This Iceberg metadata table

* The 'max_reference_age_in_ms' field indicates the maximum duration in milliseconds that a snapshot can be referenced. This age is measured from the time the snapshot was added to the table. If the age of a snapshot exceeds this duration, it will no longer be valid and will be a candidate for cleanup during maintenance operations.

* The 'min_snapshots_to_keep' field provides a lower limit on the number of snapshots to keep in the table history. The Iceberg table will always maintain at least this many snapshots, even if they are older than the 'max_snapshot_age_ms' setting.

* The 'max_snapshot_age_in_ms' field indicates the maximum age in milliseconds for any snapshot in the table. Snapshots that exceed this age could be removed by the maintenance operations, unless they are protected by the min_snapshots_to_keep setting.

The refs metadata table can be used to monitor snapshots that will be expiring soon.

```
spark.sql("""SELECT name, snapshot_id, max_snapshot_age_in_ms
FROM SPARK_CATALOG.DEFAULT.CELL_TOWERS_{}.refs
WHERE max_snapshot_age_in_ms IS NOT NULL;
""".format(USERNAME)).show()
```


You can also join snapshots to table history. For example, this query will show table history, with the application ID that wrote each snapshot:

```
select
    h.made_current_at,
    s.operation,
    h.snapshot_id,
    h.is_current_ancestor,
    s.summary['spark.app.id']
from prod.db.table.history h
join prod.db.table.snapshots s
  on h.snapshot_id = s.snapshot_id
order by made_current_at;
```


Expire snapshots:
* how far fo you want to go with timetravel
* rewrite delete files
* mow table have position delets - how often do you want to rewrite
* do you want to more often rewrite data files than manifest files
* ORC or parquet: majority of CDP is using ORC but when you migrate to iceberg it prefers parquet. Check which partitions has ORC which has parquet, how do you migrate ORC to parquet; if snowflake or dremio or others want to use files,
* want to know what is a manifest

* Maintenance data engineering: maintenace pipeline; define steps for each table; some tables you want maintenance by rewriting data files, some tables don't need to rewrite data files; If it is COW you need to expire more after, is it only insert is it delete or update? Some tables are MOR some tables are COW

* This table needs 1 day expire, this other table needs a different number of days to expire, it varies. It's not one tylenol for all tables.
* That kind of tool is not there in Cloudera domain.

Reach out to Balaji for Train the trainer training. Gave training last month.

we take backup of tabels, we send email to everyone that it's running, then we start etl for 4 hours, while it's loading

in branching you create a clone, you load data and automatically commit it, you validate it, then you merge branch with main, then it's available to everyone. You don't need to go take a backup. If an ETL fails you don't need to restore the backup table, the branch was just a branch so just ignore it. The next express snapshot will get expired. You merge branch or you drop it. 
