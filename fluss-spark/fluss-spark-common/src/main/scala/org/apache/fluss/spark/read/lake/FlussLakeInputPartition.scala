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

import org.apache.fluss.client.table.scanner.batch.dv.DvBucketPart
import org.apache.fluss.lake.source.LakeSplit
import org.apache.fluss.metadata.TableBucket
import org.apache.fluss.spark.read.FlussInputPartition

/**
 * Represents an input partition for reading data from a single lake split. Each lake split maps to
 * one Spark task, enabling parallel lake reads across splits.
 *
 * @param tableBucket
 *   the table bucket this split belongs to
 * @param lakeSplit
 *   the lake split to read
 */
case class FlussLakeInputPartition(tableBucket: TableBucket, lakeSplit: LakeSplit)
  extends FlussInputPartition {
  override def toString: String = {
    s"FlussLakeInputPartition{tableId=${tableBucket.getTableId}, bucketId=${tableBucket.getBucket}," +
      s" partitionId=${tableBucket.getPartitionId}," +
      s" lakeSplit=$lakeSplit}"
  }
}

/**
 * Represents an input partition for reading data from a lake-enabled primary key table bucket. This
 * partition combines lake snapshot splits with Fluss log tail for hybrid lake-kv reading.
 *
 * @param tableBucket
 *   the table bucket to read from
 * @param lakeSplits
 *   the lake splits to read (may be null if no lake snapshot)
 * @param logStartingOffset
 *   the log offset where incremental reading should start
 * @param logStoppingOffset
 *   the log offset where incremental reading should end
 */
case class FlussLakeUpsertInputPartition(
    tableBucket: TableBucket,
    lakeSplits: java.util.List[LakeSplit],
    logStartingOffset: Long,
    logStoppingOffset: Long)
  extends FlussInputPartition {
  override def toString: String = {
    s"FlussLakeUpsertInputPartition{tableId=${tableBucket.getTableId}, bucketId=${tableBucket.getBucket}," +
      s" partitionId=${tableBucket.getPartitionId}," +
      s" lakeSplits=${if (lakeSplits != null) lakeSplits.size() else 0}," +
      s" logStartOffset=$logStartingOffset, logStopOffset=$logStoppingOffset}"
  }
}

/**
 * Represents an input partition reading one part of a union read of a primary key table with
 * deletion vectors. A part is all of a bucket or, when the bucket's cut was pinned at planning and
 * several partitions share it, a group of its lake files or its log.
 *
 * @param part
 *   the part to read, as planned; a retried task reads it again from the start
 */
case class FlussLakeDvInputPartition(part: DvBucketPart) extends FlussInputPartition {
  override def toString: String = {
    s"FlussLakeDvInputPartition{${part.describe()}, snapshotId=${part.getSnapshotId}," +
      s" lakeSplits=${part.lakeSplitCount()}, pinned=${part.isPinned}," +
      s" cut=${part.getMaskLogEndOffset}}"
  }
}
