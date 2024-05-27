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
