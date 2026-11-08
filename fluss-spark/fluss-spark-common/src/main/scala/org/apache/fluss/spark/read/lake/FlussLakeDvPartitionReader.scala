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

package org.apache.fluss.spark.read.lake

import org.apache.fluss.client.admin.{Admin, DvAdmin}
import org.apache.fluss.client.table.scanner.DroppedPartitionDetector
import org.apache.fluss.client.table.scanner.batch.dv.{DvBucketPartReader, DvBucketPlanner, DvTableReadContext}
import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.lake.source.{LakeSource, LakeSplit}
import org.apache.fluss.metadata.TablePath
import org.apache.fluss.row.{InternalRow => FlussInternalRow}
import org.apache.fluss.spark.read.FlussPartitionReader
import org.apache.fluss.types.RowType
import org.apache.fluss.utils.{CloseableIterator, IOUtils}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.TaskFailureListener

import java.time.Duration
import java.util.Collections
import java.util.function.Supplier

/**
 * Reads one part of a union read of a primary key table with deletion vectors. It returns the
 * part's lake rows the masks do not hide, then its bucket's log up to the cut, without the versions
 * superseded before the cut.
 *
 * Spark restores no reader state, so a retried task reads its part from the start. A part planned
 * with a pinned cut reads with that cut again, as the other parts of its bucket do. The reader
 * fails if the cut is lost and cannot be derived. Planning took a hold of the cut for each part of
 * the bucket; the part releases its hold when its task attempt succeeds. A failed or killed attempt
 * releases nothing, as its retry reads with the cut again. Should two attempts of one part both
 * succeed, the cut is dropped early, and the parts that start later derive its masks from the keys,
 * as exactly.
 *
 * A part planned without a cut pins one when it starts and releases it when the reader closes,
 * which Spark does when the task ends: succeeded, failed or cancelled. Each attempt of a task pins
 * and releases its own cut.
 *
 * @param lakeSource
 *   a lake source of this reader alone: the read projects it differently for keys and rows
 */
class FlussLakeDvPartitionReader(
    tablePath: TablePath,
    partition: FlussLakeDvInputPartition,
    lakeSource: LakeSource[LakeSplit],
    projection: Array[Int],
    limit: Option[Int],
    flussConfig: Configuration)
  extends FlussPartitionReader(tablePath, flussConfig, limit)
  with Logging {

  private val partReader: DvBucketPartReader = initializeResources {
    val admin: Supplier[Admin] = () => connection.getAdmin
    val context = new DvTableReadContext(
      table,
      admin,
      lakeSource,
      projection,
      new DroppedPartitionDetector(admin, tablePath))
      .withWaitForCatchUp(
        flussConfig.get(ConfigOptions.CLIENT_SCANNER_DELETION_VECTORS_WAIT_FOR_CATCH_UP))
    val reader = new DvBucketPartReader(partition.part, context)
    try {
      reader.resolve()
    } catch {
      case error: Throwable =>
        IOUtils.closeQuietly(reader)
        throw error
    }
    reader
  }

  private var rows: CloseableIterator[FlussInternalRow] = _
  private var finished = false

  // set when the task attempt fails, before Spark closes the reader
  @volatile private var taskFailed = false
  Option(TaskContext.get()).foreach(_.addTaskFailureListener(new TaskFailureListener {
    override def onTaskFailure(context: TaskContext, error: Throwable): Unit = taskFailed = true
  }))

  override lazy val projectedRowType: RowType = rowType.project(projection)

  override def next0(): Boolean = {
    while (!closed && !finished) {
      if (rows != null && rows.hasNext) {
        currentRow = convertToSparkRow(rows.next())
        return true
      }
      // empty while the log has no records yet, null once the part is read up to its cut
      rows = partReader.pollBatch(POLL_TIMEOUT)
      finished = rows == null
    }
    false
  }

  override def close0(): Unit = {
    try {
      releaseHoldIfSucceeded()
    } finally {
      IOUtils.closeAll(rows, partReader)
    }
  }

  /** Releases the part's hold of the cut planning pinned, once its task attempt succeeded. */
  private def releaseHoldIfSucceeded(): Unit = {
    val context = TaskContext.get()
    // a reader that failed to open is closed before its task fails
    val succeeded =
      partReader != null && !taskFailed && (context == null || !context.isInterrupted())
    if (succeeded && partition.part.isPinned) {
      try {
        DvBucketPlanner.releasePinnedCuts(
          DvAdmin.of(connection.getAdmin),
          tablePath,
          Collections.singleton(partition.part),
          FlussLakeDvPartitionReader.ReleaseTimeout)
      } catch {
        case e: Exception =>
          logInfo(s"Could not release the cut of ${partition.part.describe()}; it idles out.", e)
      }
    }
  }
}

object FlussLakeDvPartitionReader {

  /** How long a part waits for its bucket's leader to take the release of its hold. */
  val ReleaseTimeout: Duration = Duration.ofSeconds(5)
}
