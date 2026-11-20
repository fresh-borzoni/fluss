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
| `lake.tiering.deletion-vectors.write-buffer-size` | MemorySize | 128mb | For tables with deletion vectors enabled. On Iceberg: the heap memory the lake writers of one tiering source task share to sort what they buffer of their rounds by primary key: the index entries of the rows of a round read through its leader's log mask, or the rows of a round in sort mode. When it is full, the writer holding the most spills its sorted entries to the local directories of `lake.tiering.io.tmp.dirs`, so a round of any size is tiered within this memory. The log masks of the rounds being read are held besides it, in at most as much heap again: a round's mask takes at most one bit per log offset of the round, about 1.25 MB for a round of 10M records, and a round whose mask does not fit waits until rounds before it end. On Paimon: the write buffer the Paimon writers of one tiering source task share, instead of a buffer of Paimon's `write-buffer-size` per bucket being tiered. When it is full, Paimon flushes the writer holding the most. Must be positive |
| `lake.tiering.deletion-vectors.max-round-records-per-bucket` | Integer | 10000000 | For Iceberg tables with deletion vectors enabled: the most log records one tiering round tiers in a bucket, so a round changes at most this many keys. The bucket's leader fixes the round's end when the round is planned, within this and `lake.tiering.deletion-vectors.max-round-bytes-per-bucket`. The round is written up to that end, and the next round, which starts at once, tiers the rest. A backlog, e.g. after the tiering service was down, is thus tiered in several rounds of bounded size. Such a round is not cut by the table's `table.datalake.freshness`, so size the caps for a round to fit in it. Must be positive |
| `lake.tiering.deletion-vectors.max-round-bytes-per-bucket` | MemorySize | 1gb | For Iceberg tables with deletion vectors enabled: the most log bytes one tiering round tiers in a bucket, as the offset index of the bucket's log places its records. A round tiers at least one record batch. Must be positive |
| `lake.tiering.deletion-vectors.round-mask.enabled` | Boolean | true | For Iceberg tables with deletion vectors enabled: whether the tiering readers read a bucket's round through its leader's log mask, the versions a later change of the round supersedes, so the round's writer writes the live versions as they come. If false, every round is written in sort mode: the writer keeps the last version of each key itself, sorting the round's rows by key within `lake.tiering.deletion-vectors.write-buffer-size`. A round whose mask does not match it is written in sort mode either way |
| `lake.tiering.deletion-vectors.round-deletes.prefetch-timeout` | Duration | 30s | For Iceberg tables with deletion vectors enabled: the tiering readers fetch the deletes of a bucket's round, the lake positions of the rows the round supersedes, from the bucket's leader while they read the round, and hand them to the committer with the round's write result. A round whose deletes are not fetched when its writer completes waits for them at most this long, and a fetch waits at most this long for a leader that is not ready. Then, or if the fetch failed, its write result goes without them, and the committer fetches them from the leader itself, as it does when a data file they name changed since they were fetched. 0 turns the readers' fetches off |
| `lake.tiering.deletion-vectors.round-deletes.prefetch-max-bytes` | MemorySize | 64mb | For Iceberg tables with deletion vectors enabled: the most bytes of round deletes a tiering reader holds at once, fetched for the rounds it reads and not yet handed to the committer. The deletes of a round that do not fit are dropped, and the committer fetches them from the leader itself |
| `lake.tiering.deletion-vectors.round-deletes.prefetch-threads` | Integer | 4 | For Iceberg tables with deletion vectors enabled: the threads a tiering reader fetches the deletes of the rounds it reads with; more fetches wait for one. Must be positive |
| `lake.tiering.deletion-vectors.writer-prepared.enabled` | Boolean | true | For Iceberg tables with deletion vectors enabled, on format version 3: whether the tiering readers prepare the deletion vectors of their buckets' rounds once the round's deletes are fetched: they merge them into the current deletion vectors of the data files they delete rows in, reading those with up to `lake.tiering.deletion-vectors.load-threads` threads, and write one Puffin file per bucket. The committer then only checks, per bucket, that each of those data files is still live on its base with the deletion vector the prepared one replaces, and commits them. A bucket whose preparation fails, whose round compacted its files, or whose prepared deletion vectors no longer hold has its deletes merged at the committer, as all buckets do if this is false; the Puffin files that are not committed are deleted |
| `lake.tiering.deletion-vectors.publish-threads` | Integer | 8 | For Iceberg tables with deletion vectors enabled: the threads the tiering committer publishes the position index parts of a round's buckets with, buckets in parallel, and fetches the deletes of the buckets whose readers did not hand them over. Must be positive |
| `lake.tiering.deletion-vectors.load-threads` | Integer | 8 | For Iceberg tables with deletion vectors enabled: the threads the tiering committer reads the current deletion vectors with of the data files a round deletes rows in, before it merges the round's deletes into them. Must be positive |
| `lake.tiering.deletion-vectors.load-max-bytes` | MemorySize | 256mb | For Iceberg tables with deletion vectors enabled: the most bytes of current deletion vectors, as their delete files give them, the tiering committer, or a tiering reader that prepares a bucket's deletion vectors, loads at once, with `lake.tiering.deletion-vectors.load-threads` threads, before it merges a round's deletes into them. The others are loaded one by one while it merges. 0 loads every one while it merges |
| `lake.tiering.deletion-vectors.cache-size` | MemorySize | 64mb | For Iceberg tables with deletion vectors enabled: the heap the tiering committer keeps the deletion vectors it wrote in, so a later round takes a data file's deletion vector from there instead of reading it, while the table's current snapshot has that deletion vector for the file. The least recently used go first. 0 turns the cache off |

The `lake.tiering.*` options are arguments of the tiering job, e.g. `--lake.tiering.deletion-vectors.write-buffer-size 256mb`, not Fluss client options. Only the Flink tiering service tiers tables with deletion vectors; see [Tables with Deletion Vectors](../../streaming-lakehouse/tiering-service.md#tables-with-deletion-vectors).

For tables with deletion vectors enabled, the tiering service writes the position index under the table's remote data directory, the one the Fluss cluster assigned the table from `remote.data.dir` or `remote.data.dirs`, where the table's tablet servers read it. The tiering service therefore needs access to that file system. With `table.datalake.auto-compaction` enabled, the tiering service compacts the lake files of these tables bucket by bucket; on Iceberg, the compaction hands the new position of every row it rewrote to the position index, so the rewrite is registered without reading it again. Lake files rewritten by another engine are re-indexed by the tiering service, which holds the next round until the rewritten files are read. A rollback of such a lake table by another engine (e.g. Iceberg's `rollback_to_snapshot`) cannot be followed: tiering of the table stops with an error naming the snapshot to make current again, and resumes once that is done. Nor can rows another engine writes to the lake table (an `INSERT`, `UPDATE`, `DELETE` or `MERGE`, i.e. an Iceberg `append`, `overwrite` or `delete` snapshot, or a Paimon `APPEND` or `OVERWRITE` commit): tiering of the table stops before writing any file, with an error naming the snapshot and the one to roll back to, and resumes once the write is rolled back. Write such changes to the Fluss table instead. Rewrites that keep the rows, such as Iceberg's `rewrite_data_files` and `rewrite_position_delete_files` or Paimon compactions, are allowed.

On Paimon, such tables are created with Paimon deletion vectors (`deletion-vectors.enabled`), and a union read uses the latest compacted snapshot: new data becomes readable from the lake once a compaction, by `table.datalake.auto-compaction` or a Paimon compaction job, moves it out of level 0, and the tiering service indexes the files of each newly readable snapshot when it commits the next round. A compaction by `table.datalake.auto-compaction` runs in the tiering service's writers: right after a writer prepares its commit, it reads back the primary keys and row positions of the files its compaction wrote and hands them to the index, so the round is not held for a re-index on the tiering readers. If the hand-off fails or does not cover every file that became readable, for example after a compaction by another engine, the tiering readers re-index the newly readable files and the round is held meanwhile.

## Data Retention

When using lakehouse storage with Union Read:
- **Expired Fluss log data** (controlled by `table.log.ttl`) remains accessible via the lake if previously tiered
- **Cleaned-up partitions** (controlled by `table.auto-partition.num-retention`) remain accessible via the lake if previously tiered

This enables Fluss to store only recent data while the lake serves as a complete historical archive.

For tables with deletion vectors enabled, Fluss also keeps the log from the readable offset of each bucket's readable lake snapshot, because tablet servers replay it to rebuild the deletion vectors after a restart or a leader change. A new leader that restores a KV snapshot with the bucket's change list replays only the log after the KV snapshot (see `kv.deletion-vectors.change-list-checkpoint.enabled`), but the log from the readable offset is still kept, as union reads read it. On Iceberg, the readable offset is the tiered offset. On Paimon, it is the first offset still in level 0: log that is tiered but not yet compacted stays in remote storage past `table.log.ttl` until a compaction makes it readable. Union reads in progress may read the log from an older offset: the masks a read pinned, and those a leader keeps for reads that do not wait, read it from the readable offset of the snapshot before a switch, so the log is kept from the lowest of these offsets. Before a bucket has a readable snapshot, and while a new leader rebuilds its deletion vectors, all of its log is kept. The bytes kept this way are reported per bucket by the `dvRetainedSize` remote log metric.
