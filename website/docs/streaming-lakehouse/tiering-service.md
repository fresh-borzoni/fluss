---
title: "Tiering Service"
sidebar_position: 2
---

# Tiering Service

The Tiering Service continuously compacts real-time data from Fluss into the configured lake format (Paimon, Iceberg, Hudi, or Lance) for cost-efficient long-term storage and analytics.

## Overview

The Tiering Service is implemented as an Apache Flink job that:
- Reads records from Fluss tables with lakehouse storage enabled
- Writes data to the configured data lake format (Paimon, Iceberg, Hudi, or Lance)
- Maintains exactly-once semantics between Fluss and the data lake
- Operates incrementally, syncing only missing data segments

:::warning
Never change the schema of a lake table managed by the Tiering Service through an external engine. The Tiering Service requires the lake table schema to stay in sync with the Fluss table schema, and a diverging schema makes the tiering job fail and restart in a loop until the schemas match again. For lake formats that support schema evolution through Fluss (Paimon and Iceberg), add columns with `ALTER TABLE` on the Fluss table instead. See [Schema Evolution](datalake-formats/paimon.md#schema-evolution) for the symptoms and the recovery steps with Paimon.
:::

For an in-depth look at the Tiering Service internals, see the blog series:

- [Tiering Service Deep Dive — Part 1](https://fluss.apache.org/blog/fluss-tiering-service-deep-dive-part1/)
- [Tiering Service Deep Dive — Part 2](https://fluss.apache.org/blog/fluss-tiering-service-deep-dive-part2/)
- [Tiering Service Deep Dive — Part 3](https://fluss.apache.org/blog/fluss-tiering-service-deep-dive-part3/)

For deployment instructions, see [Deploying Lakestream](../install-deploy/deploying-streaming-lakehouse.md).

## Architecture

The Tiering Service consists of three Flink operators:

| Operator | Description |
|----------|-------------|
| **TieringSource** | Reads records from Fluss and writes to the data lake via LakeWriter |
| **TieringCommitter** | Commits batches and advances offsets in both Fluss and lake |
| **No-Op Sink** | Dummy sink required by Flink's topology |

### How It Works

1. **Enumerator** sends heartbeat to Fluss CoordinatorService, receives table metadata
2. **SplitGenerator** calculates data delta between Fluss offsets and lake snapshot offsets
3. **Splits** are created for missing data ranges and assigned to readers
4. **Readers** fetch records from Fluss tablet servers, write via LakeWriter
5. **Committer** performs two-phase commit: first to lake, then updates Fluss coordinator
6. Cycle repeats based on `table.datalake.freshness` interval

### Split Types

| Table Type | Split Type | Description |
|------------|------------|-------------|
| Log Table (append-only) | TieringLogSplit | Defines starting and stopping offsets for a contiguous range |
| Primary Key Table | TieringSnapshotSplit | References snapshot ID and log offset for CDC replay |
| Primary Key Table with deletion vectors | TieringLogSplit | Always tiered from the changelog, never from a KV snapshot |

## Configuration Options

### Tiering Service Options

The tiering job is a standalone Flink job, and its arguments fall into three groups:

- `--fluss.*` — Fluss client configuration, e.g. `--fluss.bootstrap.servers localhost:9123`
- `--datalake.<format>.*` — lake catalog/storage configuration; Fluss strips the `datalake.<format>.` prefix before passing the remaining keys to the lake connector
- `--lake.tiering.*` — tiering job-level configuration, e.g. `--lake.tiering.auto-expire-snapshot true`

For example:

```shell
${FLINK_HOME}/bin/flink run \
    -Dparallelism.default=3 \
    /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format paimon \
    --datalake.paimon.metastore filesystem \
    --datalake.paimon.warehouse /tmp/paimon \
    --lake.tiering.auto-expire-snapshot true
```

The following `--lake.tiering.*` options are set when starting the tiering job:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `lake.tiering.auto-expire-snapshot` | Boolean | false | Auto-trigger snapshot expiration on commit, even if `table.datalake.auto-expire-snapshot` is false. On Iceberg, it applies only to [tables with deletion vectors](#tables-with-deletion-vectors); the tiering service does not expire snapshots of other Iceberg tables |
| `lake.tiering.partition.mark-done.enabled` | Boolean | false | Enable [partition mark-done](#partition-mark-done) using the actual lake table's options |
| `lake.tiering.io.tmp.dirs` | String | Flink temporary directories | Local directories used for temporary I/O files. If not configured, a `fluss` child directory under each Flink temporary directory is used. Separate multiple directories with commas or the system path separator |
| `lake.tiering.commit-thread-num` | Integer | 2 | Number of threads used to commit tables concurrently. Must be positive |

The following `lake.tiering.deletion-vectors.*` options apply only to [tables with deletion vectors](#tables-with-deletion-vectors). Like the other `lake.tiering.*` options, they are arguments of the tiering job, for example `--lake.tiering.deletion-vectors.write-buffer-size 256mb`. They are not Fluss client options and are not read from the `--fluss.*` arguments.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `lake.tiering.deletion-vectors.write-buffer-size` | MemorySize | 128mb | On Iceberg: the heap memory the lake writers of one tiering source task share to sort what they buffer of their rounds by primary key: the index entries of the rows of a round read through its leader's log mask, or the rows of a round in sort mode. When it is full, the writer holding the most spills its sorted entries to the local directories of `lake.tiering.io.tmp.dirs`, so a round of any size is tiered within this memory. The log masks of the rounds being read are held besides it, in at most as much heap again: a round's mask takes at most one bit per log offset of the round, about 1.25 MB for a round of 10M records, and a round whose mask does not fit waits until rounds before it end. On Paimon: the write buffer the Paimon writers of one tiering source task share, instead of a buffer of Paimon's `write-buffer-size` per bucket being tiered. When it is full, Paimon flushes the writer holding the most. Must be positive |
| `lake.tiering.deletion-vectors.max-round-records-per-bucket` | Integer | 10000000 | On Iceberg: the most log records one tiering round tiers in a bucket, so a round changes at most this many keys. The bucket's leader fixes the round's end when the round is planned, within this and `lake.tiering.deletion-vectors.max-round-bytes-per-bucket`. The round is written up to that end, and the next round, which starts at once, tiers the rest. A backlog is thus tiered in several rounds of bounded size. Such a round is not cut by the table's `table.datalake.freshness`, so size the caps for a round to fit in it. Must be positive |
| `lake.tiering.deletion-vectors.max-round-bytes-per-bucket` | MemorySize | 1gb | On Iceberg: the most log bytes one tiering round tiers in a bucket, as the offset index of the bucket's log places its records. A round tiers at least one record batch. See `lake.tiering.deletion-vectors.max-round-records-per-bucket`. Must be positive |
| `lake.tiering.deletion-vectors.round-mask.enabled` | Boolean | true | On Iceberg: whether the tiering readers read a bucket's round through its leader's log mask, so the round's writer writes the live versions as they come. If false, every round is written in sort mode, where the writer keeps the last version of each key itself, sorting the round's rows by key within `lake.tiering.deletion-vectors.write-buffer-size`. A round whose mask does not match it is written in sort mode either way |
| `lake.tiering.deletion-vectors.round-deletes.prefetch-timeout` | Duration | 30s | On Iceberg: the tiering readers fetch the deletes of a bucket's round, the lake positions of the rows the round supersedes, from the bucket's leader while they read the round, and hand them to the committer with the round's write result. A round whose deletes are not fetched when its writer completes waits for them at most this long, and a fetch waits at most this long for a leader that is not ready. Then, or if the fetch failed, its write result goes without them, and the committer fetches them from the leader itself, as it does when a data file they name changed since they were fetched. 0 turns the readers' fetches off |
| `lake.tiering.deletion-vectors.round-deletes.prefetch-max-bytes` | MemorySize | 64mb | On Iceberg: the most bytes of round deletes a tiering reader holds at once, fetched for the rounds it reads and not yet handed to the committer. The deletes of a round that do not fit are dropped, and the committer fetches them from the leader itself |
| `lake.tiering.deletion-vectors.round-deletes.prefetch-threads` | Integer | 4 | On Iceberg: the threads a tiering reader fetches the deletes of the rounds it reads with; more fetches wait for one. Must be positive |
| `lake.tiering.deletion-vectors.writer-prepared.enabled` | Boolean | true | On Iceberg format version 3: whether the tiering readers prepare the deletion vectors of their buckets' rounds once the round's deletes are fetched: they merge them into the current deletion vectors of the data files they delete rows in, reading those with up to `lake.tiering.deletion-vectors.load-threads` threads, and write one Puffin file per bucket. The committer then only checks, per bucket, that each of those data files is still live on its base with the deletion vector the prepared one replaces, and commits them. A bucket whose preparation fails, whose round compacted its files, or whose prepared deletion vectors no longer hold has its deletes merged at the committer, as all buckets do if this is false; the Puffin files that are not committed are deleted |
| `lake.tiering.deletion-vectors.publish-threads` | Integer | 8 | On Iceberg: the threads the tiering committer publishes the position index parts of a round's buckets with, buckets in parallel, and fetches the deletes of the buckets whose readers did not hand them over. Must be positive |
| `lake.tiering.deletion-vectors.load-threads` | Integer | 8 | On Iceberg: the threads the tiering committer reads the current deletion vectors with of the data files a round deletes rows in, before it merges the round's deletes into them. Must be positive |
| `lake.tiering.deletion-vectors.load-max-bytes` | MemorySize | 256mb | On Iceberg: the most bytes of current deletion vectors, as their delete files give them, the tiering committer, or a tiering reader that prepares a bucket's deletion vectors, loads at once, with `lake.tiering.deletion-vectors.load-threads` threads, before it merges a round's deletes into them. The others are loaded one by one while it merges. 0 loads every one while it merges |
| `lake.tiering.deletion-vectors.cache-size` | MemorySize | 64mb | On Iceberg: the heap the tiering committer keeps the deletion vectors it wrote in, so a later round takes a data file's deletion vector from there instead of reading it, while the table's current snapshot has that deletion vector for the file. The least recently used go first. 0 turns the cache off |
| `lake.tiering.deletion-vectors.reindex-files-per-split` | Integer | 16 | The most lake files one split of a re-index reads. The tiering readers re-index the files of a bucket in splits of at most this many files and `lake.tiering.deletion-vectors.reindex-rows-per-split` rows, and read the splits in parallel. Must be positive |
| `lake.tiering.deletion-vectors.reindex-rows-per-split` | Long | 4000000 | The most rows, as the lake files' metadata gives them, one split of a re-index reads, besides `lake.tiering.deletion-vectors.reindex-files-per-split`; a larger file is a split of its own. A split sorts the keys of its rows in bounded memory, spilling sorted runs to `lake.tiering.io.tmp.dirs`. Must be positive |
| `lake.tiering.deletion-vectors.trim-interval` | Integer | 20 | Every this many position index changes a table registers (on Iceberg about one per tiering round plus one per re-index or compaction, on Paimon one per advance of the readable snapshot), the tiering service removes the position index changes that no bucket with data needs any more, those before its checkpoint, and the index changes of failed rounds. It checkpoints idle buckets itself, for example those of old partitions that no round writes any more, so they do not keep the index history from being removed |
| `lake.tiering.deletion-vectors.trim-retain` | Integer | 2 | The number of index changes kept before the oldest checkpoint a bucket with data needs when the index history of a table is trimmed |
| `lake.tiering.deletion-vectors.orphan-min-age` | Duration | 6h | An index change that was never registered is removed once it is this old. It must be longer than the longest tiering round |

### Table-Level Options

The following `table.datalake.*` options are configured per table when creating or altering tables, not on the tiering job:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `table.datalake.enabled` | Boolean | false | Enable lakehouse storage for this table |
| `table.datalake.freshness` | Duration | 3min | Maximum lag between Fluss and lake data |
| `table.datalake.auto-compaction` | Boolean | false | Auto-trigger compaction in the data lake |
| `table.datalake.auto-expire-snapshot` | Boolean | false | Auto-expire snapshots in the data lake. On Iceberg, it applies only to tables with deletion vectors |
| `table.datalake.deletion-vectors.enabled` | Boolean | false | Tier the primary key table with [deletion vectors](#tables-with-deletion-vectors). Can only be set when the table is created |

## Tables with Deletion Vectors

A primary key table created with `'table.datalake.deletion-vectors.enabled' = 'true'` keeps one live row per key in every lake snapshot, and its union reads hide the lake rows of keys changed in Fluss by position instead of merging the lake and the log by key. The Tiering Service tiers such a table differently from other primary key tables:

- Each round is tiered from the changelog, never from a KV snapshot.
- On Iceberg, each bucket's leader fixes the end of the bucket's round when the round is planned, within `lake.tiering.deletion-vectors.max-round-records-per-bucket` and `lake.tiering.deletion-vectors.max-round-bytes-per-bucket`. The round is written up to that end and never stopped earlier, also not by the table's `table.datalake.freshness`, so size these caps for a round to fit in the freshness interval. If they cut a round below the end of the log, the next round starts at once.
- On Iceberg, the tiering readers read each bucket's round through its leader's log mask: the leader names the versions that a later change of the round supersedes, and the readers skip them, so the writers write the versions still live at the round's end as they come and sort only small index entries by primary key. Before writing, the readers check that the mask is of their round, and after it that every masked offset was read as a version and that the round's changes are as many as the leader counted; as they complete the round, the writers check that no key they wrote a row of came again, which would be a version the mask missed. A round whose mask does not match is written again in sort mode, where the writer keeps the last version of each key itself, sorting the round's rows by primary key, so every lake snapshot holds one live row per key (`lake.tiering.deletion-vectors.round-mask.enabled` turns the masks off). The lake positions of the rows the round supersedes come from the buckets' leaders, which derive them from the changes they keep since the readable lake snapshot: the readers fetch them while they read the round and hand them to the committer, which uses them if no snapshot since the one they were fetched on, such as the round's own compaction or another engine's rewrite, changed the data files they name or those files' deletion vectors, and otherwise, for example for a bucket whose files a compaction rewrote, fetches them from the leaders itself (`lake.tiering.deletion-vectors.round-deletes.prefetch-timeout`). On format version 3 the readers also prepare each bucket's deletion vectors from them, in one Puffin file per bucket, and the committer commits those whose data files are still live on its base with the deletion vectors they replace (`lake.tiering.deletion-vectors.writer-prepared.enabled`). The round commits its data files and the deletes of the superseded rows in one lake commit. While a leader rebuilds or replays these changes, for example after a restart or a leader change, the round waits for it, up to 10 minutes, and then runs again; the readers and the committer log each such wait.
- On Paimon, Paimon merges rows by key and writes its own deletion vectors during compactions. The committer indexes the files of each newly readable (compacted) snapshot.
- The positions are kept in a position index under the table's remote data directory, where the table's tablet servers read them, so the Tiering Service needs access to that file system.
- Lake files that another engine rewrote, for example by a compaction, are re-indexed by the tiering readers before the table's next lake commit. A rollback of the lake table past the snapshot Fluss registered, or rows another engine wrote to it, stop tiering of the table with an error. See [Lakehouse Storage](../maintenance/tiered-storage/lakehouse-storage.md#tiering-service-options).

Only the Flink Tiering Service of a Fluss version that supports deletion vectors tiers these tables. A tiering service declares this support in its heartbeats to the coordinator, and the coordinator hands tables with deletion vectors only to tiering services that declared it. A tiering job of an older Fluss version is therefore never given such a table, and the Spark tiering service skips them. Upgrade the Flink tiering job before creating tables with deletion vectors; until then, they are not tiered.

## Partition Mark-Done

The Tiering Service supports marking idle partitions of Paimon tables done. Enable the job-level option `--lake.tiering.partition.mark-done.enabled true` and configure `partition.idle-time-to-done` on the Paimon table. Runtime checks and actions use the **actual Paimon table options**. Fluss `paimon.*` properties can pass these options to Paimon when creating a table; retained Fluss properties do not override the lake table at runtime.

Only `partition.mark-done-action.mode=process-time` is supported. Other modes, including `watermark`, log a warning and disable mark-done for the table. A partition becomes eligible when both its last update and its end time are older than the configured idle duration. The default action creates a `_SUCCESS` file. Actions can be retried after failures, so custom actions must be idempotent. Late data starts tracking the partition again.

Configure `partition.time-interval` on every Paimon table using mark-done, including Fluss auto-partitioned tables. Partition end times use Paimon's timestamp extraction rules and the JVM time zone; Fluss auto-partition rules are not used. If no timestamp pattern or formatter is configured, Paimon uses its defaults.

For a `dt` partition such as `20260928`, configure the Paimon table:

```sql
ALTER TABLE events SET (
  'partition.idle-time-to-done' = '10 min',
  'partition.timestamp-pattern' = '$dt',
  'partition.timestamp-formatter' = 'yyyyMMdd',
  'partition.time-interval' = '1 d'
);
```

## Scaling

The Tiering Service is stateless and can be scaled by adjusting Flink parallelism:

```shell
${FLINK_HOME}/bin/flink run \
    -Dparallelism.default=6 \
    /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    ...
```

Multiple tiering service jobs can run simultaneously. They are coordinated by the Fluss cluster to ensure exactly-once semantics and automatic load balancing.

## Monitoring

Key metrics for monitoring the Tiering Service are available through Flink's metrics system. See [Monitoring Metrics](../maintenance/observability/monitor-metrics.md#tiering-service-metrics) for details on lakehouse tiering metrics.
