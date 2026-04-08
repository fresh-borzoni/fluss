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

import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.flink.tiering.LakeTieringJobBuilder
import org.apache.fluss.flink.tiering.source.TieringSourceOptions
import org.apache.fluss.metadata.{DataLakeFormat, TableBucket}
import org.apache.fluss.spark.FlussSparkTestBase
import org.apache.fluss.spark.SparkConnectorOptions.BUCKET_NUMBER

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.spark.sql.Row

import java.time.Duration

/**
 * Base class for lake-enabled log table read tests. Subclasses provide the lake format config and
 * lake catalog configuration.
 */
abstract class SparkLakeLogTableReadTestBase extends FlussSparkTestBase {

  protected var warehousePath: String = _

  /** The lake format used by this test. */
  protected def dataLakeFormat: DataLakeFormat

  /** Lake catalog configuration specific to the format. */
  protected def lakeCatalogConf: Configuration

  private val TIERING_PARALLELISM = 2
  private val CHECKPOINT_INTERVAL_MS = 1000L
  private val POLL_INTERVAL: Duration = Duration.ofMillis(500L)
  private val SYNC_TIMEOUT: Duration = Duration.ofMinutes(2)
  private val SYNC_POLL_INTERVAL_MS = 500L

  /** Tier all pending data for the given table to the lake. */
  protected def tierToLake(tableName: String): Unit = {
    val tableId = loadFlussTable(createTablePath(tableName)).getTableInfo.getTableId

    val execEnv = StreamExecutionEnvironment.getExecutionEnvironment
    execEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    execEnv.setParallelism(TIERING_PARALLELISM)
    execEnv.enableCheckpointing(CHECKPOINT_INTERVAL_MS)

    val flussConfig = new Configuration(flussServer.getClientConfig)
    flussConfig.set(TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL, POLL_INTERVAL)

    val jobClient = LakeTieringJobBuilder
      .newBuilder(
        execEnv,
        flussConfig,
        lakeCatalogConf,
        new Configuration(),
        dataLakeFormat.toString)
      .build()

    try {
      val tableBucket = new TableBucket(tableId, 0)
      val deadline = System.currentTimeMillis() + SYNC_TIMEOUT.toMillis
      var synced = false
      while (!synced && System.currentTimeMillis() < deadline) {
        try {
          val replica = flussServer.waitAndGetLeaderReplica(tableBucket)
          synced = replica.getLogTablet.getLakeTableSnapshotId >= 0
        } catch {
          case _: Exception =>
        }
        if (!synced) Thread.sleep(SYNC_POLL_INTERVAL_MS)
      }
      assert(synced, s"Bucket $tableBucket not synced to lake within $SYNC_TIMEOUT")
    } finally {
      jobClient.cancel().get()
    }
  }

  override protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    try {
      f
    } finally {
      tableNames.foreach(t => sql(s"DROP TABLE IF EXISTS $DEFAULT_DATABASE.$t"))
    }
  }

  test("Spark Lake Read: log table falls back when no lake snapshot") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(1, "hello"), (2, "world"), (3, "fluss")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t ORDER BY id"),
        Row(1, "hello") :: Row(2, "world") :: Row(3, "fluss") :: Nil
      )

      checkAnswer(
        sql(s"SELECT name FROM $DEFAULT_DATABASE.t ORDER BY name"),
        Row("fluss") :: Row("hello") :: Row("world") :: Nil
      )
    }
  }

  test("Spark Lake Read: log table lake-only (all data in lake, no log tail)") {
    withTable("t_lake_only") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_lake_only (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_lake_only VALUES
             |(1, "alpha"), (2, "beta"), (3, "gamma")
             |""".stripMargin)

      tierToLake("t_lake_only")

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_lake_only ORDER BY id"),
        Row(1, "alpha") :: Row(2, "beta") :: Row(3, "gamma") :: Nil
      )

      checkAnswer(
        sql(s"SELECT name FROM $DEFAULT_DATABASE.t_lake_only ORDER BY name"),
        Row("alpha") :: Row("beta") :: Row("gamma") :: Nil
      )
    }
  }

  test("Spark Lake Read: log table lake-only projection on timestamp column") {
    withTable("t_lake_timestamp") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_lake_timestamp (
             |  id INT,
             |  ts TIMESTAMP,
             |  name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_lake_timestamp VALUES
             |(1, TIMESTAMP "2026-01-01 12:00:00", "alpha"),
             |(2, TIMESTAMP "2026-01-02 12:00:00", "beta"),
             |(3, TIMESTAMP "2026-01-03 12:00:00", "gamma")
             |""".stripMargin)

      tierToLake("t_lake_timestamp")

      checkAnswer(
        sql(s"SELECT ts FROM $DEFAULT_DATABASE.t_lake_timestamp ORDER BY ts"),
        Row(java.sql.Timestamp.valueOf("2026-01-01 12:00:00")) ::
          Row(java.sql.Timestamp.valueOf("2026-01-02 12:00:00")) ::
          Row(java.sql.Timestamp.valueOf("2026-01-03 12:00:00")) :: Nil
      )
    }
  }

  test("Spark Lake Read: log table union read (lake + log tail)") {
    withTable("t_union") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_union (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_union VALUES
             |(1, "alpha"), (2, "beta"), (3, "gamma")
             |""".stripMargin)

      tierToLake("t_union")

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_union VALUES
             |(4, "delta"), (5, "epsilon")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_union ORDER BY id"),
        Row(1, "alpha") :: Row(2, "beta") :: Row(3, "gamma") ::
          Row(4, "delta") :: Row(5, "epsilon") :: Nil
      )

      checkAnswer(
        sql(s"SELECT name FROM $DEFAULT_DATABASE.t_union ORDER BY name"),
        Row("alpha") :: Row("beta") :: Row("delta") ::
          Row("epsilon") :: Row("gamma") :: Nil
      )
    }
  }

  test("Spark Lake Read: non-FULL startup mode skips lake path") {
    withTable("t_earliest") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_earliest (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()}' = '1s',
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_earliest VALUES
             |(1, "alpha"), (2, "beta"), (3, "gamma")
             |""".stripMargin)

      tierToLake("t_earliest")

      try {
        spark.conf.set("spark.sql.fluss.scan.startup.mode", "earliest")

        checkAnswer(
          sql(s"SELECT * FROM $DEFAULT_DATABASE.t_earliest ORDER BY id"),
          Row(1, "alpha") :: Row(2, "beta") :: Row(3, "gamma") :: Nil
        )
      } finally {
        spark.conf.set("spark.sql.fluss.scan.startup.mode", "full")
      }
    }
  }
}
