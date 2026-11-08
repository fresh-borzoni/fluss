---
title: Paimon
sidebar_position: 1
---

# Paimon

## Introduction

[Apache Paimon](https://paimon.apache.org/) innovatively combines a lake format with an LSM (Log-Structured Merge-tree) structure, bringing efficient updates into the lake architecture. 
To integrate Fluss with Paimon, you must enable lakehouse storage and configure Paimon as the lakehouse storage. For more details, see [Deploying Lakestream](../../install-deploy/deploying-streaming-lakehouse.md).

## Dependencies

Apache Fluss publishes the Paimon lake connector to Maven Central:

| Artifact | Jar |
|----------|-----|
| Fluss Paimon lake connector | [fluss-lake-paimon-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-lake-paimon/$FLUSS_VERSION$/fluss-lake-paimon-$FLUSS_VERSION$.jar) |

Maven coordinates:

```xml
<dependency>
  <groupId>org.apache.fluss</groupId>
  <artifactId>fluss-lake-paimon</artifactId>
  <version>$FLUSS_VERSION$</version>
</dependency>
```

Verify downloaded JARs using the [verification instructions](/downloads#verifying-downloads).

## Version Compatibility

| Use Case              | Required/Tested Versions                                          |
|-----------------------|-------------------------------------------------------------------|
| Tiering Service       | Paimon 1.4, 2.0, 2.1 (tested and verified to work)                 |
| Union Read            | Paimon 1.1, 1.2, 1.3, 1.4, 2.0, 2.1 (tested and verified to work) |
| Historical SST Lookup | Paimon 2.1.0 or later (required for concurrent lookup)             |
| Java Runtime          | Java 11 or later; Paimon 2.1 adds no higher requirement             |

## Configure Paimon as LakeHouse Storage

For general guidance on configuring Paimon as the lakehouse storage, you can refer to [Deploying Lakestream](../../install-deploy/deploying-streaming-lakehouse.md) documentation. When starting the tiering service, make sure to use Paimon-specific configurations as parameters.

### Historical Partition Access Setup

Historical partition access supports both lookups and writes. Configure `datalake.format` and the
related `datalake.paimon.*` options on every TabletServer, using the same Paimon catalog and
warehouse as the tiering service, for example:

```yaml title="server.yaml"
datalake.enabled: true
datalake.format: paimon
datalake.paimon.metastore: filesystem
datalake.paimon.warehouse: /path/to/paimon/warehouse
```

Download the [Fluss Paimon lake connector](#dependencies) and
[paimon-bundle-$PAIMON_VERSION$.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-bundle/$PAIMON_VERSION$/paimon-bundle-$PAIMON_VERSION$.jar),
and place them in `${FLUSS_HOME}/plugins/paimon/` on every TabletServer. Add any
[required catalog or storage JARs](../../install-deploy/deploying-streaming-lakehouse.md#fluss-server-jars)
to the same directory. For example, when using OSS, add `paimon-oss-<version>.jar` matching your
Paimon version. Restart the TabletServers after changing the configuration or JARs.

### Create a Paimon Table

When a table is created or altered with the option `'table.datalake.enabled' = 'true'`, Fluss will automatically create a corresponding Paimon table with the same table path by default.
Newly created Paimon tables (**clean** tables) contain only the user-defined columns of the Fluss table. Fluss no longer appends the `__bucket`, `__offset`, and `__timestamp` system columns to the physical schema.

:::note
Paimon tables created by earlier Fluss versions (**legacy** tables) still carry the three trailing system columns. These tables are **not** migrated and remain fully readable and writable. Fluss detects the layout from the physical schema — a table is treated as legacy when it carries the system columns, and clean otherwise — so both layouts are supported side by side without any manual migration.

The names `__bucket`, `__offset`, and `__timestamp` remain reserved for Fluss internal use, so user columns must not use these names.

For the rolling-upgrade requirements when moving to a Fluss version that creates clean tables, see [Upgrade Notes](../../maintenance/operations/upgrade-notes-1.0.md).
:::

```sql title="Flink SQL"
USE CATALOG fluss_catalog;

CREATE TABLE fluss_order_with_lake (
    `order_key` BIGINT,
    `cust_key` INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    `order_date` DATE,
    `order_priority` STRING,
    `clerk` STRING,
    `ptime` AS PROCTIME(),
    PRIMARY KEY (`order_key`) NOT ENFORCED
 ) WITH (
     'table.datalake.enabled' = 'true',
     'table.datalake.freshness' = '30s'
);
```

The datalake tiering service continuously tiers data from Fluss to Paimon. The parameter `table.datalake.freshness` controls the frequency that Fluss writes data to Paimon tables. By default, the data freshness is 3 minutes.

For primary key tables, changelogs are also generated in the Paimon format, enabling stream-based consumption via Paimon APIs.

### Configure a Custom Paimon Table Path

To use a different database or table name for the Paimon table, set the following options when creating the Fluss table. These options map the Fluss table to the physical Paimon database and table name; they do not rename the Fluss table:

```sql
'table.datalake.database-name' = 'paimon_database',
'table.datalake.table-name' = 'paimon_table'
```

Both options are optional. If either option is omitted, the corresponding Fluss database or table name is used. For a table created after datalake was configured for the Fluss cluster, the options can also be set in the same `ALTER TABLE` statement that enables datalake:

```sql
ALTER TABLE fluss_table SET (
  'table.datalake.enabled' = 'true',
  'table.datalake.database-name' = 'paimon_database',
  'table.datalake.table-name' = 'paimon_table'
);
```

Tables created before datalake was configured for the Fluss cluster do not support altering these options. Once the Paimon table has been created, including an automatically created Paimon table, the name mapping options cannot be modified.

### Configure Paimon Table Properties

Since Fluss version 0.7, you can also specify Paimon table properties when creating a datalake-enabled Fluss table by using the `paimon.` prefix within the Fluss table properties clause.

```sql title="Flink SQL"
CREATE TABLE fluss_order_with_lake (
    `order_key` BIGINT,
    `cust_key` INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    `order_date` DATE,
    `order_priority` STRING,
    `clerk` STRING,
    `ptime` AS PROCTIME(),
    PRIMARY KEY (`order_key`) NOT ENFORCED
 ) WITH (
     'table.datalake.enabled' = 'true',
     'table.datalake.freshness' = '30s',
     'paimon.file.format' = 'orc',
     'paimon.deletion-vectors.enabled' = 'true'
);
```

For example, you can specify the Paimon property `file.format` to change the file format of the Paimon table, or set `deletion-vectors.enabled` to enable or disable deletion vectors for the Paimon table.

### Primary Key Tables with Deletion Vectors

A primary key table created with `'table.deletion-vectors.enabled' = 'true'` uses Paimon deletion vectors, and its batch union reads hide the Paimon rows of keys changed in Fluss by position instead of merging the Paimon and Fluss data by key. Paimon merges the rows by primary key and writes the deletion vectors itself during compactions, so Fluss writes no deletes to the Paimon table.

- **Paimon options**: Fluss creates the Paimon table with `deletion-vectors.enabled` = `true`; setting `paimon.deletion-vectors.enabled` to `false` is refused. The Paimon merge engine must be `deduplicate`, the default, so that Paimon keeps the last row of each key as Fluss tiered it; another `merge-engine` and a `sequence.field` are refused. Options that make Paimon drop rows on its own are refused too: `ignore-delete` and its aliases `deduplicate.ignore-delete`, `first-row.ignore-delete` and `partial-update.ignore-delete`, and `record-level.expire-time`. These checks apply to the effective options of the Paimon table, including those it inherits from the Paimon catalog's `table-default.*` options, when the table is created and before the tiering service tiers it.
- **Partition keys**: as for any Paimon table with deletion vectors, the partition keys must be of type `STRING`.
- **Readable snapshot**: Paimon readers of a table with deletion vectors do not read level-0 files, so union reads use the latest compacted snapshot. New data becomes readable from Paimon once a compaction, by `table.datalake.auto-compaction` or a Paimon compaction job, moves it out of level 0. Until then, union reads read it from the Fluss log, which Fluss keeps for this (see [Data Retention](../../maintenance/tiered-storage/lakehouse-storage.md#data-retention)). The tiering service indexes the files of each newly readable snapshot when it commits the next round.
- **Snapshot expiry**: Fluss keeps the readable snapshot it registered last and the one before from expiring with the Paimon consumers `fluss-readable` and `fluss-readable-previous`, which also hold off Paimon's own snapshot expiration.
- **Other engines**: other engines may compact the Paimon table, and the tiering service re-indexes the files their compactions make readable. They must not write rows to it (an `APPEND` or `OVERWRITE` commit), and the table must not be rolled back past the snapshot Fluss registered; either stops tiering of the table with an error. See [Lakehouse Storage](../../maintenance/tiered-storage/lakehouse-storage.md#tiering-service-options).
- **Streaming union reads** of such a table read the Fluss KV snapshot and changelog instead of the Paimon files.

`table.deletion-vectors.enabled` can only be set when a table is created.

## Read Tables

### Reading with Apache Flink

For a table with the option `'table.datalake.enabled' = 'true'`, its data exists in two layers: one remains in Fluss, and the other has already been tiered to Paimon.  
You can choose between two views of the table:
- A **Paimon-only view**, which offers minute-level latency but better analytics performance.
- A **combined view** of both Fluss and Paimon data, which provides second-level latency but may result in slightly degraded query performance.

#### Read Data Only in Paimon

##### Prerequisites
Download the [paimon-flink.jar](https://paimon.apache.org/docs/$PAIMON_VERSION_SHORT$/project/download/) that matches your Flink version, and place it in the `FLINK_HOME/lib` directory

##### Read Paimon Data
To read only data stored in Paimon, use the `$lake` suffix in the table name. The following example demonstrates this:

```sql title="Flink SQL"
-- Assume we have a table named `orders`

-- Read from Paimon
SELECT COUNT(*) FROM orders$lake;
```

```sql title="Flink SQL"
-- We can also query the system tables
SELECT * FROM orders$lake$snapshots;
```

When you specify the `$lake` suffix in a query, the table behaves like a standard Paimon table and inherits all its capabilities.  
This allows you to take full advantage of Flink's query support and optimizations on Paimon, such as querying system tables, time travel, and more.  
For further information, refer to Paimon's [SQL Query documentation](https://paimon.apache.org/docs/$PAIMON_VERSION_SHORT$/flink/sql-query/#sql-query).

#### Union Read of Data in Fluss and Paimon

##### Prerequisites
Download the [fluss-lake-paimon-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-lake-paimon/$FLUSS_VERSION$/fluss-lake-paimon-$FLUSS_VERSION$.jar) and [paimon-bundle-$PAIMON_VERSION$.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-bundle/$PAIMON_VERSION$/paimon-bundle-$PAIMON_VERSION$.jar), and place it into `${FLINK_HOME}/lib`.

##### Union Read
To read the full dataset, which includes both Fluss (fresh) and Paimon (historical) data, simply query the table without any suffix. The following example illustrates this:

```sql title="Flink SQL"
-- Set execution mode to streaming or batch, here just take batch as an example
SET 'execution.runtime-mode' = 'batch';

-- Query will union data from Fluss and Paimon
SELECT SUM(order_count) AS total_orders FROM ads_nation_purchase_power;
```
It supports both batch and streaming modes, using Paimon for historical data and Fluss for fresh data:
- In batch mode

  The query may run slower than reading only from Paimon because it needs to merge rows from both Paimon and Fluss. However, it returns the most up-to-date results. Multiple executions of the query may produce different outputs due to continuous data ingestion.

- In streaming mode

  Flink first reads the latest Paimon snapshot (tiered via tiering service), then switches to Fluss starting from the log offset aligned with that snapshot, ensuring exactly-once semantics.
  This design enables Fluss to store only a small portion of the dataset in the Fluss cluster, reducing costs, while Paimon serves as the source of complete historical data when needed. 

Key behavior for data retention:
- **Expired Fluss log data** (controlled by `table.log.ttl`) remains accessible via Paimon if previously tiered
- **Cleaned-up partitions** in partitioned tables (controlled by `table.auto-partition.num-retention`) remain accessible via Paimon if previously tiered

### Reading with other Engines

Since the data tiered to Paimon from Fluss is stored as a standard Paimon table, you can use any engine that supports Paimon to read it. Below is an example using [StarRocks](https://paimon.apache.org/docs/$PAIMON_VERSION_SHORT$/ecosystem/starrocks/):

First, create a Paimon catalog in StarRocks:

```sql title="StarRocks SQL"
CREATE EXTERNAL CATALOG paimon_catalog
PROPERTIES (
       "type" = "paimon",
       "paimon.catalog.type" = "filesystem",
       "paimon.catalog.warehouse" = "/tmp/paimon_data_warehouse"
);
```

> **NOTE**: The configuration values for `paimon.catalog.type` and `paimon.catalog.warehouse` must match those used when configuring Paimon as the lakehouse storage for Fluss in `server.yaml`.

Then, you can query the `orders` table using StarRocks:

```sql title="StarRocks SQL"
-- The table is in the database `fluss`
SELECT COUNT(*) FROM paimon_catalog.fluss.orders;
```

```sql title="StarRocks SQL"
-- Query the system tables to view snapshots of the table
SELECT * FROM paimon_catalog.fluss.enriched_orders$snapshots;
```

## Schema Evolution

The schema of a Paimon table managed by Fluss must always be evolved through Fluss. When you add columns to a Fluss table with `ALTER TABLE ... ADD` (see [Add Columns](../../engine-flink/ddl.md#add-columns)), the new columns are appended at the end as nullable columns, and Fluss applies the same change to the Paimon table as part of the `ALTER TABLE` statement, so the two schemas stay in sync.

:::warning External schema changes stall tiering
Do not change the schema of a Fluss-managed Paimon table through an external engine, for example by adding a column on the Paimon table from Spark or Doris. The tiering service requires the user columns of the Paimon table to match the Fluss table schema. Once the Paimon table contains a column that the Fluss table does not have, tiering the table's records can fail with an error like the following:

```
Caused by: java.io.IOException: Failed to write Fluss record to Paimon.
Caused by: java.lang.IllegalStateException: Field 18 is NULL because Paimon schema is wider than Fluss record.
```

The tiering job then fails and restarts in a loop until the schemas match again, cycling between the RUNNING and RESTARTING states in the Flink UI. The restart loop also interrupts tiering progress for the other tables served by the same job. Adding a column is the most common trigger, but any external change that makes the schemas diverge stops tiering in the same way, possibly with a different error message.

No data is lost or corrupted, and the records that could not be tiered remain readable in Fluss. However, Fluss keeps log data until it has been tiered, so log retention for the table is effectively paused and storage usage grows until the schemas match again. Fix the mismatch promptly.

To recover, make the two schemas consistent again. To keep the externally added column, run a matching `ALTER TABLE ... ADD` statement on the Fluss table. When the Paimon table already contains the column in the expected position, Fluss completes the change without touching the Paimon table. If Fluss rejects the statement because the schemas cannot be reconciled, drop the externally added column from the Paimon table and, if you still need it, add it through Fluss afterwards. Once the schemas match, the tiering job recovers on its next automatic restart and the pending records are tiered completely. If you cancelled the tiering job in the meantime, resubmit it.
:::

## Rescaling Bucket Count

For a datalake-enabled Fluss table with Paimon as the lake format, `ALTER TABLE ... SET ('bucket.num' = N)` is also propagated to the Paimon table: the `bucket` option of the Paimon table is updated as part of the same statement, before the Fluss-side metadata is committed (if the propagation fails, the whole `ALTER TABLE` fails and neither side is changed). As on the Fluss side, the new count applies only to partitions created afterwards — existing partitions keep their original bucket count, and no existing data or lake files are rewritten. See [Rescaling Bucket Count for Future Partitions](../../table-design/data-distribution/bucketing.md#rescaling-bucket-count-for-future-partitions) for the full semantics and constraints.

## Data Type Mapping

When integrating with Paimon, Fluss automatically converts between Fluss data types and Paimon data types.  
The following table shows the mapping between [Fluss data types](../../table-design/data-types.md) and Paimon data types:

| Fluss Data Type                                                 | Paimon Data Type                                                |
|-----------------------------------------------------------------|-----------------------------------------------------------------|
| BOOLEAN                                                         | BOOLEAN                                                         |
| TINYINT                                                         | TINYINT                                                         |
| SMALLINT                                                        | SMALLINT                                                        |
| INT                                                             | INT                                                             |
| BIGINT                                                          | BIGINT                                                          |
| FLOAT                                                           | FLOAT                                                           |
| DOUBLE                                                          | DOUBLE                                                          |
| DECIMAL                                                         | DECIMAL                                                         |
| STRING                                                          | STRING                                                          |
| CHAR                                                            | CHAR                                                            |
| DATE                                                            | DATE                                                            |
| TIME                                                            | TIME                                                            |
| TIMESTAMP                                                       | TIMESTAMP                                                       |
| TIMESTAMP WITH LOCAL TIMEZONE                                   | TIMESTAMP WITH LOCAL TIMEZONE                                   |
| BINARY                                                          | BINARY                                                          |
| BYTES                                                           | BYTES                                                           |
| ARRAY\<t\>                                                      | ARRAY\<t\>                                                      |
| MAP\<kt, vt\>                                                   | MAP\<kt, vt\>                                                   |
| ROW\<n0 t0, n1 t1, ...\><br/>ROW\<n0 t0 'd0', n1 t1 'd1', ...\> | ROW\<n0 t0, n1 t1, ...\><br/>ROW\<n0 t0 'd0', n1 t1 'd1', ...\> |

## Snapshot Metadata

Fluss adds specific metadata to Paimon snapshots for traceability:

- **commit-user**: Set to `__fluss_lake_tiering` to identify Fluss-generated snapshots
- **fluss-offsets**: JSON string containing the Fluss bucket offset mapping to track the tiering progress

#### Non-Partitioned Tables

For non-partitioned tables, the metadata structure of `fluss-offsets` is:

```json
[
  {"bucket": 0, "offset": 1234},
  {"bucket": 1, "offset": 5678},
  {"bucket": 2, "offset": 9012}
]
```

#### Partitioned Tables

For partitioned tables, the metadata structure includes partition information:

```json
[
  {
    "partition_name": "date=2025",
    "partition_id": 0,
    "bucket": 0,
    "offset": 3
  },
  {
    "partition_name": "date=2025",
    "partition_id": 1,
    "bucket": 0,
    "offset": 3
  }
]
```

#### Metadata Fields Explanation

| Field            | Description                                  | Example                      |
|------------------|----------------------------------------------|------------------------------|
| `partition_id`   | Unique identifier in Fluss for the partition | `0`, `1`                     |
| `bucket`         | Bucket identifier within the partition       | `0`, `1`, `2`                |
| `partition_name` | Human-readable partition name                | `"date=2025"`, `"date=2026"` |
| `offset`         | Offset within the partition's log            | `3`, `1000`                  |
