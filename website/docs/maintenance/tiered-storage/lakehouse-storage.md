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
| `table.datalake.auto-expire-snapshot` | Boolean | false | Auto-expire snapshots in the data lake. On Paimon, with the table's Paimon snapshot retention options; on Iceberg, at most every 10 minutes after a commit, the snapshots older than the table's `history.expire.max-snapshot-age-ms` (1 hour if unset) beyond its last `history.expire.min-snapshots-to-keep` (10 if unset), deleting the files no snapshot left references. Snapshots Fluss still needs are kept: on Iceberg, the readable snapshots it tags (`fluss-readable-*`) and, for tables with deletion vectors, the oldest snapshot of the position index history and those after it; on Paimon tables with deletion vectors, the last two readable snapshots Fluss registered and those after them, which the consumers `fluss-readable` and `fluss-readable-previous` keep from expiring. |

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
| `lake.tiering.auto-expire-snapshot` | Boolean | false | Auto-trigger snapshot expiration on commit, even if `table.datalake.auto-expire-snapshot` is false |
| `lake.tiering.partition.mark-done.enabled` | Boolean | false | Enable [partition mark-done](../../streaming-lakehouse/tiering-service.md#partition-mark-done) using the actual lake table's options |
| `lake.tiering.io.tmp.dirs` | String | Flink temporary directories | Local directories used for temporary I/O files. If not configured, a `fluss` child directory under each Flink temporary directory is used. Separate multiple directories with commas or the system path separator |
| `lake.tiering.commit-thread-num` | Integer | 2 | Number of threads used to commit tables concurrently. Must be positive |
| `lake.tiering.deletion-vectors.trim-interval` | Integer | 20 | For tables with deletion vectors enabled: every this many tiering rounds of a table, position index changes no bucket with data needs any more (those before its checkpoint) are removed, together with index changes of failed rounds. The tiering service checkpoints idle buckets itself, e.g. those of old partitions no round writes any more, so they do not keep the index history from being removed |
| `lake.tiering.deletion-vectors.trim-retain` | Integer | 2 | The number of index changes kept before the oldest checkpoint a bucket with data needs when the index history of a table is trimmed |
| `lake.tiering.deletion-vectors.orphan-min-age` | Duration | 6h | An index change that was never registered is removed once it is this old. Must be longer than the longest tiering round |
| `lake.tiering.deletion-vectors.reindex-files-per-split` | Integer | 16 | For tables with deletion vectors enabled: the most rewritten lake files one split of a re-index reads. Files of a bucket another engine rewrote are re-indexed in splits of at most this many files and `lake.tiering.deletion-vectors.reindex-rows-per-split` rows, read in parallel by the tiering readers. Must be positive |
| `lake.tiering.deletion-vectors.reindex-rows-per-split` | Long | 4000000 | For tables with deletion vectors enabled: the most rows, as the lake files' metadata gives them, one split of a re-index reads; a larger file is a split of its own. A split sorts the keys of its rows in bounded memory, spilling sorted runs to `lake.tiering.io.tmp.dirs`. Must be positive |
| `lake.tiering.deletion-vectors.write-buffer-size` | MemorySize | 128mb | For tables with deletion vectors enabled: the heap memory the lake writers of one tiering source task share to sort the rows of their rounds by primary key. When it is full, the writer holding the most spills its sorted rows to the local directories of `lake.tiering.io.tmp.dirs`, so a round of any size is tiered within this memory. Must be positive |
| `lake.tiering.deletion-vectors.max-round-keys-per-bucket` | Integer | 10000000 | For tables with deletion vectors enabled: the most primary keys one tiering round changes in a bucket. The commit of a round looks up and deletes the superseded rows of all keys it changed; once a bucket reaches this many keys, or `lake.tiering.deletion-vectors.max-round-bytes-per-bucket`, its round ends at the record just tiered and the next round, which starts at once, tiers the rest. A backlog, e.g. after the tiering service was down, is thus tiered in several rounds of bounded size. Must be positive |
| `lake.tiering.deletion-vectors.max-round-bytes-per-bucket` | MemorySize | 1gb | For tables with deletion vectors enabled: the most bytes of changed rows one tiering round holds for a bucket, in memory and spilled to local disk, in their encoded size. Must be positive |

For tables with deletion vectors enabled, the tiering service writes the position index under the Fluss remote data directory, so it must be started with the same `remote.data.dir` as the Fluss cluster. With `table.datalake.auto-compaction` enabled, the tiering service compacts the lake files of these tables bucket by bucket; on Iceberg, the compaction hands the new position of every row it rewrote to the position index, so the rewrite is registered without reading it again. Lake files rewritten by another engine are re-indexed by the tiering service, which holds the next round until the rewritten files are read. A rollback of such a lake table by another engine (e.g. Iceberg's `rollback_to_snapshot`) cannot be followed: tiering of the table stops with an error naming the snapshot to make current again, and resumes once that is done. Nor can rows another engine writes to the lake table (an `INSERT`, `UPDATE`, `DELETE` or `MERGE`, i.e. an Iceberg `append`, `overwrite` or `delete` snapshot, or a Paimon `APPEND` or `OVERWRITE` commit): tiering of the table stops before writing any file, with an error naming the snapshot and the one to roll back to, and resumes once the write is rolled back. Write such changes to the Fluss table instead. Rewrites that keep the rows, such as Iceberg's `rewrite_data_files` and `rewrite_position_delete_files` or Paimon compactions, are allowed.

On Paimon, such tables are created with Paimon deletion vectors (`deletion-vectors.enabled`), and a union read uses the latest compacted snapshot: new data becomes readable from the lake once a compaction, by `table.datalake.auto-compaction` or a Paimon compaction job, moves it out of level 0, and the tiering service indexes the files of each newly readable snapshot when it commits the next round. A compaction by `table.datalake.auto-compaction` runs in the tiering service's writers, which hand the positions of the rows they wrote to the index, so its files are not read again; those of a compaction by another engine are.

## Data Retention

When using lakehouse storage with Union Read:
- **Expired Fluss log data** (controlled by `table.log.ttl`) remains accessible via the lake if previously tiered
- **Cleaned-up partitions** (controlled by `table.auto-partition.num-retention`) remain accessible via the lake if previously tiered

This enables Fluss to store only recent data while the lake serves as a complete historical archive.
