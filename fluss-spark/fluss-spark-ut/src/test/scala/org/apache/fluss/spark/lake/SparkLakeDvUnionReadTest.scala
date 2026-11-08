/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.spark.lake

import org.apache.fluss.client.ConnectionFactory
import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPart
import org.apache.fluss.client.table.writer.UpsertWriter
import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.flink.tiering.LakeTieringJobBuilder
import org.apache.fluss.flink.tiering.source.TieringSourceOptions
import org.apache.fluss.metadata.{DataLakeFormat, PartitionSpec, Schema, TableBucket, TableDescriptor, TablePath}
import org.apache.fluss.server.kv.dv.DerivedDvManager
import org.apache.fluss.spark.SparkFlussConf
import org.apache.fluss.spark.read.FlussUpsertScan
import org.apache.fluss.spark.read.lake.{FlussLakeDvInputPartition, FlussLakeInputPartition, FlussLakeUpsertInputPartition}
import org.apache.fluss.spark.utils.FlussConnectionCache
import org.apache.fluss.testutils.DataTestUtils.row
import org.apache.fluss.types.DataTypes
import org.apache.fluss.utils.IOUtils

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.core.execution.JobClient
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

import java.nio.file.Files
import java.time.Duration
import java.util.Collections
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.util.Try

/**
 * Union reads of primary key tables with deletion vectors: lake rows minus the masks of the rows
 * the log changed, plus the log after the readable snapshot. The Flink tiering service tiers the
 * tables, as only it derives the deletion vectors of a round.
 */
abstract class SparkLakeDvUnionReadTestBase extends SparkLakeTableReadTestBase {

  /** A kind of lake table with deletion vectors. */
  protected case class DvVariant(name: String, properties: Map[String, String])

  protected val Buckets = 2

  private val MaskCacheMs = 300L

  /** The kinds of lake tables with deletion vectors this lake format has. */
  protected def variants: Seq[DvVariant]

  /** Whether the lake plans the files of a bucket's tiering rounds as splits of their own. */
  protected def keepsFilesOfRounds: Boolean

  /** Whether a union read of a table without deletion vectors can merge its lake rows by key. */
  protected def mergesWithoutDeletionVectors: Boolean

  private var tieringJob: JobClient = _

  // Each test tiers its table with a job of its own, cancelled before the table is dropped: a job
  // re-indexing the files of a dropped table fails.
  override protected def withTable(tableNames: String*)(f: => Unit): Unit =
    super.withTable(tableNames: _*) {
      tieringJob = startTieringJob()
      try {
        f
      } finally {
        stopTieringJob()
      }
    }

  private def startTieringJob(): JobClient = {
    val tieringConfig = new Configuration(flussServer.getClientConfig)
    // derived rounds write the position index under the remote data dir
    tieringConfig.set(ConfigOptions.REMOTE_DATA_DIR, flussServer.getRemoteDataDir)
    tieringConfig.set(TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL, Duration.ofMillis(500))
    // options of the tiering service, not of its Fluss client
    val lakeTieringConfig = new Configuration()
    lakeTieringConfig.set(
      ConfigOptions.LAKE_TIERING_DELETION_VECTORS_REINDEX_FILES_PER_SPLIT,
      Integer.valueOf(1))
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    env.setParallelism(2)
    LakeTieringJobBuilder
      .newBuilder(env, tieringConfig, lakeCatalogConf, lakeTieringConfig, dataLakeFormat.toString)
      .build()
  }

  private def stopTieringJob(): Unit = {
    try {
      tieringJob.cancel().get(1, TimeUnit.MINUTES)
    } catch {
      // ended already, which the waits for tiering reported
      case _: Exception =>
    } finally {
      tieringJob = null
    }
  }

  /** The rows of a table by key and partition. */
  private class TableRows(partitioned: Boolean) {
    val rows: mutable.Map[(Int, String), String] = mutable.Map.empty

    def upsert(writer: UpsertWriter, id: Int, name: String, pt: String): Unit = {
      writer.upsert(flussRow(id, name, pt))
      rows((id, pt)) = name
    }

    def delete(writer: UpsertWriter, id: Int, pt: String): Unit = {
      writer.delete(flussRow(id, "deleted", pt))
      rows.remove((id, pt))
    }

    def expected: Seq[Row] = rows.toSeq.map {
      case ((id, pt), name) => if (partitioned) Row(id, name, pt) else Row(id, name)
    }

    private def flussRow(id: Int, name: String, pt: String) =
      if (partitioned) row(Int.box(id), name, pt) else row(Int.box(id), name)
  }

  private def createDvTable(
      tableName: String,
      variant: DvVariant,
      partitioned: Boolean,
      deletionVectors: Boolean = true): TablePath = {
    val tablePath = createTablePath(tableName)
    val schema =
      Schema.newBuilder().column("id", DataTypes.INT()).column("name", DataTypes.STRING())
    val builder = TableDescriptor
      .builder()
      .distributedBy(Buckets)
      .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
      .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key(), "500ms")
      .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), deletionVectors.toString)
    variant.properties.foreach {
      case (k, v) =>
        if (k.startsWith("table.")) builder.property(k, v) else builder.customProperty(k, v)
    }
    if (partitioned) {
      schema.column("pt", DataTypes.STRING()).primaryKey("id", "pt")
      builder.partitionedBy("pt")
    } else {
      schema.primaryKey("id")
    }
    admin.createTable(tablePath, builder.schema(schema.build()).build(), false).get()
    if (partitioned) {
      Seq("p0", "p1").foreach {
        pt =>
          admin
            .createPartition(
              tablePath,
              new PartitionSpec(Collections.singletonMap("pt", pt)),
              false)
            .get()
      }
    }
    tablePath
  }

  private def write(tablePath: TablePath)(f: UpsertWriter => Unit): Unit = {
    val table = loadFlussTable(tablePath)
    try {
      val writer = table.newUpsert().createWriter()
      f(writer)
      writer.flush()
    } finally {
      table.close()
    }
    // Leaders answer masks they built up to 100 ms before, so a read right after the writes
    // could be cut before them.
    Thread.sleep(MaskCacheMs)
  }

  /** The high watermarks of the buckets of the table. */
  private def highWatermarks(tablePath: TablePath): Map[TableBucket, Long] = {
    val tableInfo = admin.getTableInfo(tablePath).get()
    val partitionIds: Seq[java.lang.Long] =
      if (tableInfo.isPartitioned) {
        admin
          .listPartitionInfos(tablePath)
          .get()
          .asScala
          .map(p => Long.box(p.getPartitionId))
          .toList
      } else {
        Seq(null)
      }
    partitionIds.flatMap {
      partitionId =>
        (0 until Buckets).map {
          bucket =>
            val tableBucket = new TableBucket(tableInfo.getTableId, partitionId, bucket)
            tableBucket -> flussServer.waitAndGetLeaderReplica(tableBucket).getLogHighWatermark
        }
    }.toMap
  }

  /** Fails the test at once if the tiering job ended, as nothing gets tiered any more. */
  private def checkTieringJob(): Unit = {
    val ended =
      try {
        tieringJob.getJobStatus.get(1, TimeUnit.MINUTES).isTerminalState
      } catch {
        // the job's mini cluster shuts down with it
        case _: Exception => true
      }
    if (ended) {
      val failure =
        try {
          tieringJob.getJobExecutionResult.get(1, TimeUnit.MINUTES)
          null
        } catch {
          case e: Exception => e
        }
      throw new AssertionError("The tiering job ended", failure)
    }
  }

  private def readableSnapshotId(tablePath: TablePath): Option[Long] =
    try {
      Some(admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId)
    } catch {
      case _: Exception => None
    }

  /** Upserts a tick row until the readable snapshot advances; returns the new one. */
  private def tickUntilReadableAdvances(
      tablePath: TablePath,
      rows: TableRows,
      previous: Option[Long],
      pt: String): Long = {
    val deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(3)
    while (System.nanoTime() < deadline) {
      checkTieringJob()
      val readable = readableSnapshotId(tablePath)
      if (readable.isDefined && readable != previous) {
        return readable.get
      }
      write(tablePath)(rows.upsert(_, -1, "tick", pt))
      Thread.sleep(1000)
    }
    fail(s"The readable snapshot of $tablePath is stuck at $previous")
  }

  /** Waits until the readable snapshot covers every write; returns it. */
  private def waitUntilAllReadable(tablePath: TablePath): Long = {
    val deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(3)
    while (System.nanoTime() < deadline) {
      checkTieringJob()
      try {
        val snapshot = admin.getReadableLakeSnapshot(tablePath).get()
        val offsets = snapshot.getTableBucketsOffset
        val covered = highWatermarks(tablePath).forall {
          case (bucket, highWatermark) =>
            highWatermark == 0 || offsets.getOrDefault(bucket, 0L) >= highWatermark
        }
        if (covered) {
          return snapshot.getSnapshotId
        }
      } catch {
        case _: Exception => // nothing readable yet
      }
      Thread.sleep(500)
    }
    fail(s"Writes of $tablePath are not all readable from the lake")
  }

  private def dvPartitions(tableName: String): Seq[DvBucketPart] =
    lakeInputPartitions(sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName")).toSeq.collect {
      case p: FlussLakeDvInputPartition => p.part
    }

  private def assertUnionRead(tableName: String, rows: TableRows): Unit = {
    checkAnswer(sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName"), rows.expected)
    assert(dvPartitions(tableName).nonEmpty, s"$tableName is not read with deletion vectors")
  }

  /** The SQL conf of `client.scanner.deletion-vectors.wait-for-catch-up`. */
  private val WaitForCatchUp = SparkFlussConf.SPARK_FLUSS_CONF_PREFIX +
    ConfigOptions.CLIENT_SCANNER_DELETION_VECTORS_WAIT_FOR_CATCH_UP.key()

  /** The lake snapshots the leaders of the table's buckets cover, -1 for none. */
  private def coveredSnapshots(tablePath: TablePath): Seq[Long] =
    highWatermarks(tablePath).keys.toSeq.map {
      bucket =>
        val kvTablet = flussServer.waitAndGetLeaderReplica(bucket).getKvTablet
        if (kvTablet == null) -1L else kvTablet.getDvManager.getReadableSnapshotId
    }

  /** The cuts the leaders of the table's buckets hold for union reads. */
  private def pinnedCuts(tablePath: TablePath): Int =
    highWatermarks(tablePath).keys.toSeq.map {
      bucket =>
        val kvTablet = flussServer.waitAndGetLeaderReplica(bucket).getKvTablet
        if (kvTablet == null || kvTablet.getDvManager == null) 0
        else kvTablet.getDvManager.pinnedCutCount()
    }.sum

  private def waitUntil(what: String, timeout: Duration)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + timeout.toNanos
    while (!condition) {
      if (System.nanoTime() > deadline) {
        fail(s"Timed out waiting until $what")
      }
      Thread.sleep(20)
    }
  }

  /** Reads the table in Spark tasks that take `millisPerRow` per row. */
  private def slowRead(tableName: String, millisPerRow: Long): Seq[(Int, String)] =
    sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName").rdd
      .map {
        r =>
          Thread.sleep(millisPerRow)
          (r.getInt(0), r.getString(1))
      }
      .collect()
      .toSeq

  /**
   * Restarts the tablet servers, so new leaders rebuild the deletion vectors of their buckets. The
   * clients get new connections, as old ones keep the restarted servers' endpoints.
   */
  private def restartTabletServers(): Unit = {
    (0 until 3).foreach(server => flussServer.restartTabletServer(server, new Configuration()))
    flussServer.assertHasTabletServerNumber(3)
    IOUtils.closeAll(admin, conn)
    conn = ConnectionFactory.createConnection(flussServer.getClientConfig)
    admin = conn.getAdmin
    FlussConnectionCache.clearIdleConnections()
  }

  private def readAll(
      reader: PartitionReader[org.apache.spark.sql.catalyst.InternalRow]): Seq[(Int, String)] = {
    try {
      val rows = mutable.ArrayBuffer[(Int, String)]()
      while (reader.next()) {
        val row = reader.get()
        rows += ((row.getInt(0), row.getUTF8String(1).toString))
      }
      rows.toList
    } finally {
      reader.close()
    }
  }

  variants.foreach {
    variant =>
      test(s"Spark DV union read (${variant.name}): updates and deletes across tiering rounds") {
        val tableName = s"dv_rounds_${variant.name}"
        withTable(tableName) {
          val tablePath = createDvTable(tableName, variant, partitioned = false)
          val rows = new TableRows(false)

          write(tablePath)(w => (0 until 20).foreach(i => rows.upsert(w, i, s"v_$i", null)))
          var readable = tickUntilReadableAdvances(tablePath, rows, None, null)
          assertUnionRead(tableName, rows)

          // updates, deletes and a delete followed by a re-insert of tiered keys
          write(tablePath) {
            w =>
              (0 until 5).foreach(i => rows.upsert(w, i, s"updated_$i", null))
              (5 until 8).foreach(i => rows.delete(w, i, null))
              rows.delete(w, 8, null)
              rows.upsert(w, 8, "reinserted_8", null)
          }
          // the masks hide the tiered versions before the changes are tiered
          assertUnionRead(tableName, rows)
          readable = tickUntilReadableAdvances(tablePath, rows, Some(readable), null)
          assertUnionRead(tableName, rows)

          // changes of keys changed in the last round, and new keys
          write(tablePath) {
            w =>
              (0 until 2).foreach(i => rows.upsert(w, i, s"again_$i", null))
              rows.delete(w, 10, null)
              rows.delete(w, 8, null)
              (30 until 35).foreach(i => rows.upsert(w, i, s"new_$i", null))
          }
          assertUnionRead(tableName, rows)
          waitUntilAllReadable(tablePath)
          assertUnionRead(tableName, rows)
        }
      }

      test(s"Spark DV union read (${variant.name}): partitioned table") {
        val tableName = s"dv_partitioned_${variant.name}"
        withTable(tableName) {
          val tablePath = createDvTable(tableName, variant, partitioned = true)
          val rows = new TableRows(true)

          write(tablePath) {
            w =>
              for (pt <- Seq("p0", "p1"); i <- 0 until 10) {
                rows.upsert(w, i, s"v_${i}_$pt", pt)
              }
          }
          val readable = tickUntilReadableAdvances(tablePath, rows, None, "p0")
          assertUnionRead(tableName, rows)

          write(tablePath) {
            w =>
              rows.upsert(w, 1, "updated_1_p0", "p0")
              rows.delete(w, 2, "p0")
              rows.upsert(w, 3, "updated_3_p1", "p1")
              rows.delete(w, 4, "p1")
              rows.upsert(w, 20, "new_20_p1", "p1")
          }
          assertUnionRead(tableName, rows)
          checkAnswer(
            sql(s"SELECT id, name FROM $DEFAULT_DATABASE.$tableName WHERE pt = 'p1'"),
            rows.rows.toSeq.collect { case ((id, "p1"), name) => Row(id, name) })

          tickUntilReadableAdvances(tablePath, rows, Some(readable), "p0")
          waitUntilAllReadable(tablePath)
          assertUnionRead(tableName, rows)

          // a partition dropped from Fluss is read from the lake alone
          admin
            .dropPartition(
              tablePath,
              new PartitionSpec(Collections.singletonMap("pt", "p1")),
              false)
            .get()
          flussServer.waitUntilPartitionsDropped(tablePath, Collections.singletonList("p1"))
          val df = sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName")
          checkAnswer(df, rows.expected)
          assert(lakeInputPartitions(df).exists(_.isInstanceOf[FlussLakeInputPartition]))
        }
      }

      test(s"Spark DV union read (${variant.name}): projection, filter and limit") {
        val tableName = s"dv_projection_${variant.name}"
        withTable(tableName) {
          val tablePath = createDvTable(tableName, variant, partitioned = false)
          val rows = new TableRows(false)
          write(tablePath)(w => (0 until 20).foreach(i => rows.upsert(w, i, s"v_$i", null)))
          tickUntilReadableAdvances(tablePath, rows, None, null)
          write(tablePath) {
            w =>
              (0 until 10).foreach(i => rows.upsert(w, i, s"updated_$i", null))
              rows.delete(w, 12, null)
          }

          checkAnswer(
            sql(s"SELECT name, id FROM $DEFAULT_DATABASE.$tableName WHERE id >= 5"),
            rows.rows.toSeq.collect { case ((id, _), name) if id >= 5 => Row(name, id) })
          checkAnswer(
            sql(s"SELECT id FROM $DEFAULT_DATABASE.$tableName WHERE name LIKE 'updated%'"),
            (0 until 10).map(Row(_)))
          checkAnswer(
            sql(s"SELECT count(*) FROM $DEFAULT_DATABASE.$tableName"),
            Row(rows.rows.size.toLong) :: Nil)
          assert(sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName LIMIT 3").collect().length == 3)
        }
      }

      test(s"Spark DV union read (${variant.name}): retried parts read their pinned cut again") {
        val tableName = s"dv_retry_${variant.name}"
        withTable(tableName) {
          val tablePath = createDvTable(tableName, variant, partitioned = false)
          val rows = new TableRows(false)
          write(tablePath)(w => (0 until 20).foreach(i => rows.upsert(w, i, s"v_$i", null)))
          var readable = tickUntilReadableAdvances(tablePath, rows, None, null)
          write(tablePath)(w => (0 until 10).foreach(i => rows.upsert(w, i, s"updated_$i", null)))
          readable = tickUntilReadableAdvances(tablePath, rows, Some(readable), null)
          write(tablePath) {
            w =>
              (10 until 15).foreach(i => rows.upsert(w, i, s"again_$i", null))
              rows.delete(w, 0, null)
          }

          withSQLConf(
            SparkFlussConf.SPARK_FLUSS_CONF_PREFIX +
              ConfigOptions.CLIENT_SCANNER_DELETION_VECTORS_FILE_GROUP_ROWS.key() -> "1") {
            val scan = flussScan(sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName")).collectFirst {
              case s: FlussUpsertScan => s
            }.get
            val batch = scan.toBatch
            val partitions = batch.planInputPartitions()
            val factory: PartitionReaderFactory = batch.createReaderFactory()
            val dvPartitions = partitions.toSeq.collect { case p: FlussLakeDvInputPartition => p }
            assert(dvPartitions.nonEmpty, s"no parts: ${partitions.toSeq}")
            val expected = rows.rows.toSeq.map { case ((id, _), name) => (id, name) }.sorted
            // a bucket read in one part is not pinned at planning: its part pins when it starts
            val pinnedKeys = dvPartitions
              .filter(_.part.isPinned)
              .flatMap(p => readAll(factory.createReader(p)).map(_._1))
              .toSet

            // changes after planning are past the pinned cuts
            write(tablePath) {
              w =>
                (0 until 20).filter(pinnedKeys.contains).foreach {
                  i => rows.upsert(w, i, s"late_$i", null)
                }
                if (pinnedKeys.contains(19)) {
                  rows.delete(w, 19, null)
                }
            }

            val read = partitions.toSeq.flatMap {
              partition =>
                // a task that fails after some rows is retried from the start
                val failed = factory.createReader(partition)
                try {
                  failed.next()
                } finally {
                  failed.close()
                }
                readAll(factory.createReader(partition))
            }
            assert(read.sorted == expected)
          }
          checkAnswer(sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName"), rows.expected)
        }
      }
  }

  test("Spark DV union read: a bucket's lake files are read by several partitions") {
    assume(keepsFilesOfRounds, s"$dataLakeFormat plans a bucket's files as one split")
    val variant = variants.head
    val tableName = "dv_file_groups"
    withTable(tableName) {
      val tablePath = createDvTable(tableName, variant, partitioned = false)
      val rows = new TableRows(false)
      var readable: Option[Long] = None
      for (round <- 0 until 3) {
        write(tablePath) {
          w =>
            (0 until 10).foreach(i => rows.upsert(w, i + round * 5, s"v_${round}_$i", null))
            if (round > 0) rows.delete(w, round, null)
        }
        readable = Some(tickUntilReadableAdvances(tablePath, rows, readable, null))
      }
      write(tablePath)(w => (0 until 4).foreach(i => rows.upsert(w, i * 7, s"log_$i", null)))

      withSQLConf(
        SparkFlussConf.SPARK_FLUSS_CONF_PREFIX +
          ConfigOptions.CLIENT_SCANNER_DELETION_VECTORS_FILE_GROUP_ROWS.key() -> "1") {
        val parts = dvPartitions(tableName)
        val partsPerBucket = parts.groupBy(_.getTableBucket).values.map(_.size)
        assert(partsPerBucket.exists(_ > 1), s"no bucket read in several parts: $parts")
        assert(parts.exists(_.getFileGroup >= 0), s"no file group parts: $parts")
        assertUnionRead(tableName, rows)
      }
    }
  }

  test("Spark DV union read: a split bucket's cut is released as its parts end") {
    assume(keepsFilesOfRounds, s"$dataLakeFormat plans a bucket's files as one split")
    val variant = variants.head
    val tableName = "dv_split_bucket_release"
    withTable(tableName) {
      val tablePath = createDvTable(tableName, variant, partitioned = false)
      val rows = new TableRows(false)
      var readable: Option[Long] = None
      for (round <- 0 until 3) {
        write(tablePath) {
          w =>
            (0 until 10).foreach(i => rows.upsert(w, i + round * 5, s"v_${round}_$i", null))
            if (round > 0) rows.delete(w, round, null)
        }
        readable = Some(tickUntilReadableAdvances(tablePath, rows, readable, null))
      }
      write(tablePath)(w => (0 until 4).foreach(i => rows.upsert(w, i * 7, s"log_$i", null)))
      assert(pinnedCuts(tablePath) == 0)

      withSQLConf(
        SparkFlussConf.SPARK_FLUSS_CONF_PREFIX +
          ConfigOptions.CLIENT_SCANNER_DELETION_VECTORS_FILE_GROUP_ROWS.key() -> "1") {
        val df = sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName")
        // planning pins each split bucket's cut once, however often Spark plans the scan
        val parts = lakeInputPartitions(df).toSeq.collect {
          case p: FlussLakeDvInputPartition => p.part
        }
        assert(parts.exists(p => p.isPinned && p.sharesBucket), s"no split bucket: $parts")
        val pinned = pinnedCuts(tablePath)
        assert(pinned > 0)
        assert(lakeInputPartitions(df).length == parts.length)
        assert(pinnedCuts(tablePath) == pinned)

        assert(df.collect().map(_.toString).sorted.toSeq == rows.expected.map(_.toString).sorted)
        // each part released its hold as its task succeeded: no cut is left to idle out
        waitUntil("the read released the cuts of its split buckets", Duration.ofSeconds(30)) {
          pinnedCuts(tablePath) == 0
        }
      }
    }
  }

  test("Spark DV union read: reads that do not wait while the buckets move to new snapshots") {
    val variant = variants.head
    val tableName = "dv_no_wait_moves"
    withTable(tableName) {
      val tablePath = createDvTable(tableName, variant, partitioned = false)
      val rows = new TableRows(false)
      write(tablePath)(w => (0 until 20).foreach(i => rows.upsert(w, i, s"v_$i", null)))
      var readable = tickUntilReadableAdvances(tablePath, rows, None, null)
      for (round <- 0 until 3) {
        write(tablePath) {
          w =>
            (0 until 5).foreach(i => rows.upsert(w, i + round, s"r${round}_$i", null))
            rows.delete(w, 10 + round, null)
        }
        // a read that waits: the leaders serve no older cut than this to the reads after it
        checkAnswer(sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName"), rows.expected)
        readable = tickUntilReadableAdvances(tablePath, rows, Some(readable), null)
        // Right after the registration the leaders still catch up or derive their masks again,
        // so these reads take the snapshot before, the masks ready, or fold the log by key. Only
        // the tick row changed after the read that waited, and it may be read a moment older.
        val expected = rows.expected.filter(_.getInt(0) >= 0)
        withSQLConf(WaitForCatchUp -> "false") {
          for (_ <- 0 until 3) {
            val read = sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName").collect().toSeq
            assert(read.exists(_.getInt(0) == -1), s"the tick row is missing: $read")
            assert(
              read.filter(_.getInt(0) >= 0).map(_.toString).sorted ==
                expected.map(_.toString).sorted)
          }
        }
      }
      checkAnswer(sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName"), rows.expected)
    }
  }

  test("Spark DV union read: the cuts a read pinned are released when its tasks end") {
    val variant = variants.head
    val tableName = "dv_released_cuts"
    withTable(tableName) {
      val tablePath = createDvTable(tableName, variant, partitioned = false)
      val rows = new TableRows(false)
      write(tablePath)(w => (0 until 20).foreach(i => rows.upsert(w, i, s"v_$i", null)))
      tickUntilReadableAdvances(tablePath, rows, None, null)
      write(tablePath) {
        w =>
          (0 until 5).foreach(i => rows.upsert(w, i, s"updated_$i", null))
          rows.delete(w, 7, null)
      }
      val expected = rows.rows.toSeq.map { case ((id, _), name) => (id, name) }.sorted
      assert(pinnedCuts(tablePath) == 0)
      implicit val ec: ExecutionContext = ExecutionContext.global

      // a read holds the cuts of its buckets while it runs, and releases them as its tasks end,
      // long before the leaders' idle timeout
      val finished = Future(slowRead(tableName, 100))
      waitUntil("the read pinned its cuts", Duration.ofMinutes(1))(pinnedCuts(tablePath) > 0)
      assert(Await.result(finished, ScalaDuration(2, TimeUnit.MINUTES)).sorted == expected)
      waitUntil("the finished read released its cuts", Duration.ofSeconds(30)) {
        pinnedCuts(tablePath) == 0
      }

      // a cancelled read releases them too
      val cancelled = Future {
        spark.sparkContext.setJobGroup("dv-cancelled-read", "cancelled", interruptOnCancel = true)
        try {
          slowRead(tableName, 1000)
        } finally {
          spark.sparkContext.clearJobGroup()
        }
      }
      waitUntil("the read pinned its cuts", Duration.ofMinutes(1))(pinnedCuts(tablePath) > 0)
      spark.sparkContext.cancelJobGroup("dv-cancelled-read")
      assert(Try(Await.result(cancelled, ScalaDuration(2, TimeUnit.MINUTES))).isFailure)
      waitUntil("the cancelled read released its cuts", Duration.ofSeconds(30)) {
        pinnedCuts(tablePath) == 0
      }
    }
  }

  test("Spark DV union read: tables without deletion vectors are merged by key as before") {
    val tableName = "no_dv"
    withTable(tableName) {
      val tablePath =
        createDvTable(tableName, variants.head, partitioned = false, deletionVectors = false)
      val rows = new TableRows(false)
      write(tablePath)(w => (0 until 10).foreach(i => rows.upsert(w, i, s"v_$i", null)))
      waitUntilAllReadable(tablePath)
      write(tablePath)(w => rows.upsert(w, 1, "updated_1", null))

      val df = sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName")
      val partitions: Seq[InputPartition] = lakeInputPartitions(df).toSeq
      assert(partitions.nonEmpty)
      assert(partitions.forall(_.isInstanceOf[FlussLakeUpsertInputPartition]), partitions)
      if (mergesWithoutDeletionVectors) {
        checkAnswer(df, rows.expected)
      }
    }
  }

  // Restarts the tablet servers: the last test of the suite.
  test("Spark DV union read: a read that does not wait while the leaders rebuild reads by key") {
    val variant = variants.head
    val tableName = "dv_rebuilding"
    withTable(tableName) {
      val tablePath = createDvTable(tableName, variant, partitioned = false)
      val rows = new TableRows(false)
      write(tablePath)(w => (0 until 20).foreach(i => rows.upsert(w, i, s"v_$i", null)))
      tickUntilReadableAdvances(tablePath, rows, None, null)
      write(tablePath) {
        w =>
          (0 until 5).foreach(i => rows.upsert(w, i, s"updated_$i", null))
          rows.delete(w, 7, null)
      }

      // the new leaders' rebuilds wait until the gate opens
      val rebuilds = new CountDownLatch(1)
      flussServer.holdDvRebuilds(rebuilds)
      try {
        restartTabletServers()
        // writes go on while the leaders rebuild
        write(tablePath) {
          w =>
            (5 until 10).foreach(i => rows.upsert(w, i, s"during_$i", null))
            rows.delete(w, 11, null)
        }
        // the new leaders have no masks to serve, so the read can only be read by key
        assert(coveredSnapshots(tablePath).forall(_ == -1L))
        withSQLConf(WaitForCatchUp -> "false") {
          val df = sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName")
          val parts = lakeInputPartitions(df).toSeq.collect {
            case p: FlussLakeDvInputPartition => p.part
          }
          // a bucket pinned at planning is planned folded, the others fold when they start
          assert(
            parts.nonEmpty && parts.forall(
              p => !p.isPinned || p.getReadMode == DvBucketPart.ReadMode.FOLDED),
            s"parts not read by key: $parts")
          checkAnswer(df, rows.expected)
        }
        assert(coveredSnapshots(tablePath).forall(_ == -1L))
      } finally {
        flussServer.holdDvRebuilds(null)
        rebuilds.countDown()
      }

      // once the leaders rebuilt, a read that waits reads with their masks
      val df = sql(s"SELECT * FROM $DEFAULT_DATABASE.$tableName")
      checkAnswer(df, rows.expected)
      assert(
        lakeInputPartitions(df).toSeq
          .collect { case p: FlussLakeDvInputPartition => p.part.getReadMode }
          .forall(_ != DvBucketPart.ReadMode.FOLDED))
    }
  }
}

@SparkLakeTest
@SparkLakeIcebergTest
class SparkLakeIcebergDvUnionReadTest extends SparkLakeDvUnionReadTestBase {

  override protected def dataLakeFormat: DataLakeFormat = DataLakeFormat.ICEBERG

  // v2 masks lake rows with position delete files, v3 with deletion vectors
  override protected def variants: Seq[DvVariant] = Seq(
    DvVariant("iceberg_v3", Map("iceberg.format-version" -> "3")),
    DvVariant("iceberg_v2", Map("iceberg.format-version" -> "2")))

  override protected def keepsFilesOfRounds: Boolean = true

  // Iceberg reads rows in no key order, which a merge by key needs
  override protected def mergesWithoutDeletionVectors: Boolean = false

  override protected def flussConf: Configuration = {
    val conf = super.flussConf
    conf.setString("datalake.format", DataLakeFormat.ICEBERG.toString)
    conf.setString("datalake.iceberg.type", "hadoop")
    warehousePath =
      Files.createTempDirectory("fluss-testing-iceberg-dv-read").resolve("warehouse").toString
    conf.setString("datalake.iceberg.warehouse", warehousePath)
    conf
  }

  override protected def lakeCatalogConf: Configuration = {
    val conf = new Configuration()
    conf.setString("type", "hadoop")
    conf.setString("warehouse", warehousePath)
    conf
  }
}

@SparkLakeTest
class SparkLakePaimonDvUnionReadTest extends SparkLakeDvUnionReadTestBase {

  override protected def dataLakeFormat: DataLakeFormat = DataLakeFormat.PAIMON

  // the readable snapshot of a Paimon table with deletion vectors is its latest compaction
  override protected def variants: Seq[DvVariant] = Seq(
    DvVariant("paimon", Map(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key() -> "true")))

  override protected def keepsFilesOfRounds: Boolean = false

  override protected def mergesWithoutDeletionVectors: Boolean = true

  override protected def flussConf: Configuration = {
    val conf = super.flussConf
    conf.setString("datalake.format", DataLakeFormat.PAIMON.toString)
    conf.setString("datalake.paimon.metastore", "filesystem")
    conf.setString("datalake.paimon.cache-enabled", "false")
    warehousePath =
      Files.createTempDirectory("fluss-testing-paimon-dv-read").resolve("warehouse").toString
    conf.setString("datalake.paimon.warehouse", warehousePath)
    conf
  }

  override protected def lakeCatalogConf: Configuration = {
    val conf = new Configuration()
    conf.setString("metastore", "filesystem")
    conf.setString("warehouse", warehousePath)
    conf
  }
}
