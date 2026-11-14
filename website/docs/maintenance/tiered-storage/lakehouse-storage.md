---
title: "Lakehouse Storage"
sidebar_position: 3
---

# Lakehouse Storage

Fluss leverages well-known Lakehouse storage solutions like Apache Paimon, Apache Iceberg, Apache Hudi, and Lance as the tiered storage layer. The Tiering Service continuously tiers Fluss data to Lakehouse storage, where it can be read by Fluss clients in a streaming manner and accessed directly by external systems such as Flink, Spark, StarRocks, and others.

For deployment instructions, see [Deploying Lakestream](../../install-deploy/deploying-streaming-lakehouse.md).

For architecture details, see [Tiering Service](../../streaming-lakehouse/tiering-service.md).

## Dependencies

Apache Fluss publishes the Flink-based tiering service JAR to Maven Central:

| Artifact | Jar |
|----------|-----|
| Fluss Flink tiering service | [fluss-flink-tiering-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-flink-tiering/$FLUSS_VERSION$/fluss-flink-tiering-$FLUSS_VERSION$.jar) |

Maven coordinates:

```xml
<dependency>
  <groupId>org.apache.fluss</groupId>
  <artifactId>fluss-flink-tiering</artifactId>
  <version>$FLUSS_VERSION$</version>
</dependency>
```

Verify downloaded JARs using the [verification instructions](/downloads#verifying-downloads).

## Cluster Configuration

Lakehouse storage is configured in `server.yaml` using the `datalake.` prefix:

```yaml title="server.yaml"
datalake.format: paimon
datalake.paimon.metastore: filesystem
datalake.paimon.warehouse: /tmp/paimon
```

Fluss processes configurations by removing the `datalake.<format>.` prefix and uses the remaining configuration to create the data lake catalog.

For format-specific configuration, see:
- [Paimon](../../streaming-lakehouse/datalake-formats/paimon.md)
- [Iceberg](../../streaming-lakehouse/datalake-formats/iceberg.md)
- [Hudi](../../streaming-lakehouse/datalake-formats/hudi.md)
- [Lance](../../streaming-lakehouse/datalake-formats/lance.md)

## Table-Level Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `table.datalake.enabled` | Boolean | false | Enable lakehouse storage for this table |
| `table.datalake.freshness` | Duration | 3min | Maximum lag between Fluss and data lake table |
| `table.datalake.format` | String | - | Data lake format (paimon, iceberg, hudi, lance). Inherits from cluster config |
| `table.datalake.auto-compaction` | Boolean | false | Auto-trigger compaction in the data lake |
| `table.datalake.auto-compaction.interval` | Duration | 10min | For tables with deletion vectors: the least time between two compactions of a bucket's lake files |
| `table.datalake.auto-compaction.min-small-files` | Integer | 3 | For tables with deletion vectors: the small lake files (below 1/16 of the target file size) a bucket needs before they are merged |
| `table.datalake.auto-expire-snapshot` | Boolean | false | Auto-expire snapshots in the data lake. On Paimon, with the table's Paimon snapshot retention options. On Iceberg, only tables with deletion vectors are expired, and the tiering service does not expire snapshots of other Iceberg tables: at most every 10 minutes after a commit, it expires the snapshots older than the table's `history.expire.max-snapshot-age-ms` (1 hour if unset) beyond its last `history.expire.min-snapshots-to-keep` (10 if unset), deleting the files no snapshot left references. Snapshots Fluss still needs are kept: on Iceberg, the readable snapshots it tags (`fluss-readable-*`), and the oldest snapshot of the position index history and those after it; on Paimon tables with deletion vectors, the last two readable snapshots Fluss registered and those after them, which the consumers `fluss-readable` and `fluss-readable-previous` keep from expiring. |

Example:
```sql title="Flink SQL"
CREATE TABLE my_table (
    id BIGINT PRIMARY KEY NOT ENFORCED,
    name STRING
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '1min'
);
```

## Tiering Service Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `lake.tiering.auto-expire-snapshot` | Boolean | false | Auto-trigger snapshot expiration on commit, even if `table.datalake.auto-expire-snapshot` is false. On Iceberg, it applies only to tables with deletion vectors |
| `lake.tiering.partition.mark-done.enabled` | Boolean | false | Enable [partition mark-done](../../streaming-lakehouse/tiering-service.md#partition-mark-done) using the actual lake table's options |
| `lake.tiering.io.tmp.dirs` | String | Flink temporary directories | Local directories used for temporary I/O files. If not configured, a `fluss` child directory under each Flink temporary directory is used. Separate multiple directories with commas or the system path separator |
| `lake.tiering.commit-thread-num` | Integer | 2 | Number of threads used to commit tables concurrently. Must be positive |
| `lake.tiering.deletion-vectors.trim-interval` | Integer | 20 | For tables with deletion vectors enabled: every this many position index changes a table registers (on Iceberg about one per tiering round plus one per re-index or compaction, on Paimon one per advance of the readable snapshot), position index changes no bucket with data needs any more (those before its checkpoint) are removed, together with index changes of failed rounds. The tiering service checkpoints idle buckets itself, e.g. those of old partitions no round writes any more, so they do not keep the index history from being removed |
| `lake.tiering.deletion-vectors.trim-retain` | Integer | 2 | The number of index changes kept before the oldest checkpoint a bucket with data needs when the index history of a table is trimmed |
| `lake.tiering.deletion-vectors.orphan-min-age` | Duration | 6h | An index change that was never registered is removed once it is this old. Must be longer than the longest tiering round |
| `lake.tiering.deletion-vectors.reindex-files-per-split` | Integer | 16 | For tables with deletion vectors enabled: the most lake files one split of a re-index reads. Files of a bucket another engine rewrote, or on Paimon the files of a newly readable snapshot, are re-indexed in splits of at most this many files and `lake.tiering.deletion-vectors.reindex-rows-per-split` rows, read in parallel by the tiering readers. Must be positive |
| `lake.tiering.deletion-vectors.reindex-rows-per-split` | Long | 4000000 | For tables with deletion vectors enabled: the most rows, as the lake files' metadata gives them, one split of a re-index reads; a larger file is a split of its own. A split sorts the keys of its rows in bounded memory, spilling sorted runs to `lake.tiering.io.tmp.dirs`. Must be positive |
| `lake.tiering.deletion-vectors.write-buffer-size` | MemorySize | 128mb | For Iceberg tables with deletion vectors enabled: the heap memory the lake writers of one tiering source task share to sort the rows of their rounds by primary key. When it is full, the writer holding the most spills its sorted rows to the local directories of `lake.tiering.io.tmp.dirs`, so a round of any size is tiered within this memory. Must be positive |
| `lake.tiering.deletion-vectors.max-round-keys-per-bucket` | Integer | 10000000 | For Iceberg tables with deletion vectors enabled: the most primary keys one tiering round changes in a bucket. The commit of a round looks up and deletes the superseded rows of all keys it changed; once a bucket reaches this many keys, or `lake.tiering.deletion-vectors.max-round-bytes-per-bucket`, its round ends at the record just tiered and the next round, which starts at once, tiers the rest. A backlog, e.g. after the tiering service was down, is thus tiered in several rounds of bounded size. Must be positive |
| `lake.tiering.deletion-vectors.max-round-bytes-per-bucket` | MemorySize | 1gb | For Iceberg tables with deletion vectors enabled: the most bytes of changed rows one tiering round holds for a bucket, in memory and spilled to local disk, in their encoded size. Must be positive |

The `lake.tiering.*` options are arguments of the tiering job, e.g. `--lake.tiering.deletion-vectors.write-buffer-size 256mb`, not Fluss client options. Only the Flink tiering service tiers tables with deletion vectors; see [Tables with Deletion Vectors](../../streaming-lakehouse/tiering-service.md#tables-with-deletion-vectors).

For tables with deletion vectors enabled, the tiering service writes the position index under the table's remote data directory, the one the Fluss cluster assigned the table from `remote.data.dir` or `remote.data.dirs`, where the table's tablet servers read it. The tiering service therefore needs access to that file system. With `table.datalake.auto-compaction` enabled, the tiering service compacts the lake files of these tables bucket by bucket; on Iceberg, the compaction hands the new position of every row it rewrote to the position index, so the rewrite is registered without reading it again. Lake files rewritten by another engine are re-indexed by the tiering service, which holds the next round until the rewritten files are read. A rollback of such a lake table by another engine (e.g. Iceberg's `rollback_to_snapshot`) cannot be followed: tiering of the table stops with an error naming the snapshot to make current again, and resumes once that is done. Nor can rows another engine writes to the lake table (an `INSERT`, `UPDATE`, `DELETE` or `MERGE`, i.e. an Iceberg `append`, `overwrite` or `delete` snapshot, or a Paimon `APPEND` or `OVERWRITE` commit): tiering of the table stops before writing any file, with an error naming the snapshot and the one to roll back to, and resumes once the write is rolled back. Write such changes to the Fluss table instead. Rewrites that keep the rows, such as Iceberg's `rewrite_data_files` and `rewrite_position_delete_files` or Paimon compactions, are allowed.

On Paimon, such tables are created with Paimon deletion vectors (`deletion-vectors.enabled`), and a union read uses the latest compacted snapshot: new data becomes readable from the lake once a compaction, by `table.datalake.auto-compaction` or a Paimon compaction job, moves it out of level 0, and the tiering service indexes the files of each newly readable snapshot when it commits the next round. A compaction by `table.datalake.auto-compaction` runs in the tiering service's writers: right after a writer prepares its commit, it reads back the primary keys and row positions of the files its compaction wrote and hands them to the index, so the round is not held for a re-index on the tiering readers. If the hand-off fails or does not cover every file that became readable, for example after a compaction by another engine, the tiering readers re-index the newly readable files and the round is held meanwhile.

## Data Retention

When using lakehouse storage with Union Read:
- **Expired Fluss log data** (controlled by `table.log.ttl`) remains accessible via the lake if previously tiered
- **Cleaned-up partitions** (controlled by `table.auto-partition.num-retention`) remain accessible via the lake if previously tiered

This enables Fluss to store only recent data while the lake serves as a complete historical archive.

For tables with deletion vectors enabled, Fluss also keeps the log from the readable offset of each bucket's readable lake snapshot, because tablet servers replay it to rebuild the deletion vectors after a restart or a leader change. A new leader that restores a KV snapshot with the bucket's change list replays only the log after the KV snapshot (see `kv.deletion-vectors.change-list-checkpoint.enabled`), but the log from the readable offset is still kept, as union reads read it. On Iceberg, the readable offset is the tiered offset. On Paimon, it is the first offset still in level 0: log that is tiered but not yet compacted stays in remote storage past `table.log.ttl` until a compaction makes it readable. Union reads in progress may read the log from an older offset: the masks a read pinned, and those a leader keeps for reads that do not wait, read it from the readable offset of the snapshot before a switch, so the log is kept from the lowest of these offsets. Before a bucket has a readable snapshot, and while a new leader rebuilds its deletion vectors, all of its log is kept. The bytes kept this way are reported per bucket by the `dvRetainedSize` remote log metric.
