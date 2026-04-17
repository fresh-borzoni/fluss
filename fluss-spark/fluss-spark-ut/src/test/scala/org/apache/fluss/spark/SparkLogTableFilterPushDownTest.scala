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

package org.apache.fluss.spark

import org.apache.fluss.spark.read.FlussAppendScan

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation}
import org.apache.spark.sql.sources.Filter
import org.assertj.core.api.Assertions.assertThat

/**
 * End-to-end tests for Fluss log-table filter pushdown. Assert both result correctness and that the
 * intended filters reach the scan (Spark re-applies everything, so correctness alone can't prove
 * pushdown is happening).
 */
class SparkLogTableFilterPushDownTest extends FlussSparkTestBase {

  test("Equality on integer column is pushed and produces correct rows") {
    withSampleTable {
      val query =
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t WHERE amount = 603 ORDER BY orderId")
      checkAnswer(query, Row(800L, 23L, 603, "addr3") :: Nil)
      assertPushedFilterTypes(query, Set("EqualTo"))
    }
  }

  test("Range predicate on bigint column is pushed") {
    withSampleTable {
      val query =
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t WHERE orderId >= 900 ORDER BY orderId")
      checkAnswer(
        query,
        Row(900L, 24L, 604, "addr4") ::
          Row(1000L, 25L, 605, "addr5") :: Nil)
      assertPushedFilterTypes(query, Set("GreaterThanOrEqual"))
    }
  }

  test("AND of two pushable predicates is pushed") {
    withSampleTable {
      val query = sql(s"""
                         |SELECT * FROM $DEFAULT_DATABASE.t
                         |WHERE orderId >= 700 AND amount < 605
                         |ORDER BY orderId""".stripMargin)
      checkAnswer(
        query,
        Row(700L, 22L, 602, "addr2") ::
          Row(800L, 23L, 603, "addr3") ::
          Row(900L, 24L, 604, "addr4") :: Nil)
      val pushed = pushedFilters(query).map(_.getClass.getSimpleName)
      assert(pushed.contains("GreaterThanOrEqual") && pushed.contains("LessThan"))
    }
  }

  test("IS NULL / IS NOT NULL on string column is pushed") {
    withSampleTable {
      val query =
        sql(s"SELECT COUNT(*) FROM $DEFAULT_DATABASE.t WHERE address IS NOT NULL")
      checkAnswer(query, Row(5L) :: Nil)
      assertPushedFilterTypes(query, Set("IsNotNull"))
    }
  }

  test("LIKE 'prefix%' is pushed as StringStartsWith") {
    withSampleTable {
      val query =
        sql(s"SELECT orderId FROM $DEFAULT_DATABASE.t WHERE address LIKE 'addr%' ORDER BY orderId")
      checkAnswer(query, Row(600L) :: Row(700L) :: Row(800L) :: Row(900L) :: Row(1000L) :: Nil)
      assertPushedFilterTypes(query, Set("StringStartsWith"))
    }
  }

  test("IN on string column is pushed") {
    withSampleTable {
      val query = sql(s"""
                         |SELECT orderId FROM $DEFAULT_DATABASE.t
                         |WHERE address IN ('addr1', 'addr3')
                         |ORDER BY orderId""".stripMargin)
      checkAnswer(query, Row(600L) :: Row(800L) :: Nil)
      val pushed = pushedFilters(query).map(_.getClass.getSimpleName)
      // Spark may rewrite small INs as EqualTo/Or.
      assertThat(pushed.exists(name => name == "In" || name == "EqualTo" || name == "Or")).isTrue
    }
  }

  test("Non-pushable predicate falls back to Spark (correctness preserved)") {
    withSampleTable {
      val query =
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t WHERE amount % 2 = 0 ORDER BY orderId")
      checkAnswer(
        query,
        Row(700L, 22L, 602, "addr2") ::
          Row(900L, 24L, 604, "addr4") :: Nil)
      val pushed = pushedFilters(query).map(_.getClass.getSimpleName)
      assertThat(pushed.exists(_ == "EqualTo")).isFalse
    }
  }

  test("Mixed pushable + non-pushable: pushable half gets pushed, result correct") {
    withSampleTable {
      val query = sql(s"""
                         |SELECT * FROM $DEFAULT_DATABASE.t
                         |WHERE orderId >= 700 AND amount % 2 = 0
                         |ORDER BY orderId""".stripMargin)
      checkAnswer(
        query,
        Row(700L, 22L, 602, "addr2") ::
          Row(900L, 24L, 604, "addr4") :: Nil)
      val pushed = pushedFilters(query).map(_.getClass.getSimpleName)
      assert(pushed.contains("GreaterThanOrEqual"))
    }
  }

  test("DATE and TIMESTAMP_NTZ literals are pushed") {
    withTable("typed") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.typed (
             |  id INT,
             |  dt DATE,
             |  ts TIMESTAMP_NTZ
             |)""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.typed VALUES
             |(1, DATE '2026-01-01', TIMESTAMP_NTZ '2026-01-01 12:00:00'),
             |(2, DATE '2026-01-02', TIMESTAMP_NTZ '2026-01-02 12:00:00'),
             |(3, DATE '2026-01-03', TIMESTAMP_NTZ '2026-01-03 12:00:00')
             |""".stripMargin)

      val byDate =
        sql(s"SELECT id FROM $DEFAULT_DATABASE.typed WHERE dt = DATE '2026-01-02'")
      checkAnswer(byDate, Row(2) :: Nil)
      assertPushedFilterTypes(byDate, Set("EqualTo"))

      val byTs = sql(s"""SELECT id FROM $DEFAULT_DATABASE.typed
                        |WHERE ts >= TIMESTAMP_NTZ '2026-01-02 00:00:00'""".stripMargin)
      checkAnswer(byTs, Row(2) :: Row(3) :: Nil)
      assertPushedFilterTypes(byTs, Set("GreaterThanOrEqual"))
    }
  }

  private def withSampleTable(body: => Unit): Unit = withTable("t") {
    sql(s"""
           |CREATE TABLE $DEFAULT_DATABASE.t (
           |  orderId BIGINT,
           |  itemId  BIGINT,
           |  amount  INT,
           |  address STRING
           |)""".stripMargin)
    sql(s"""
           |INSERT INTO $DEFAULT_DATABASE.t VALUES
           |(600L, 21L, 601, 'addr1'), (700L, 22L, 602, 'addr2'),
           |(800L, 23L, 603, 'addr3'), (900L, 24L, 604, 'addr4'),
           |(1000L, 25L, 605, 'addr5')
           |""".stripMargin)
    body
  }

  private def pushedFilters(df: DataFrame): Array[Filter] = {
    // Walk both plans — AQE can hide the v2 scan under a query-stage relation.
    df.queryExecution.executedPlan.foreach(_ => ())
    val scans =
      df.queryExecution.executedPlan.collect {
        case b: BatchScanExec => b.scan
      } ++ df.queryExecution.optimizedPlan.collect {
        case DataSourceV2ScanRelation(_, scan, _, _, _) => scan
      }
    scans
      .collect { case f: FlussAppendScan => f.pushedSparkFilters }
      .flatten
      .toArray
  }

  private def assertPushedFilterTypes(df: DataFrame, expected: Set[String]): Unit = {
    val pushed = pushedFilters(df).map(_.getClass.getSimpleName).toSet
    assert(
      expected.exists(pushed.contains),
      s"Expected any of $expected in pushed filters, got $pushed")
  }
}
