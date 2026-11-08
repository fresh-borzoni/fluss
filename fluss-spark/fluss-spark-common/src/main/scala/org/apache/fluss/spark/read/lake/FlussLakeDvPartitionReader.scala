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

import org.apache.fluss.client.admin.Admin
import org.apache.fluss.client.table.scanner.DroppedPartitionDetector
import org.apache.fluss.client.table.scanner.batch.dv.{DvBucketPartReader, DvTableReadContext}
import org.apache.fluss.config.Configuration
import org.apache.fluss.lake.source.{LakeSource, LakeSplit}
import org.apache.fluss.metadata.TablePath
import org.apache.fluss.row.{InternalRow => FlussInternalRow}
import org.apache.fluss.spark.read.FlussPartitionReader
import org.apache.fluss.types.RowType
import org.apache.fluss.utils.{CloseableIterator, IOUtils}

import org.apache.spark.internal.Logging

import java.util.function.Supplier

/**
 * Reads one part of a union read of a primary key table with deletion vectors. It returns the
 * part's lake rows the masks do not hide, then its bucket's log up to the cut, without the versions
 * superseded before the cut.
 *
 * Spark restores no reader state, so a retried task reads its part from the start. A part planned
 * with a pinned cut reads with that cut again, as the other parts of its bucket do. The reader
 * fails if the cut is lost and cannot be derived.
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
    IOUtils.closeAll(rows, partReader)
  }
}
