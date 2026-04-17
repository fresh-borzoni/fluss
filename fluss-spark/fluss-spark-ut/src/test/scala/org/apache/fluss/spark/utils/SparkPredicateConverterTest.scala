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

package org.apache.fluss.spark.utils

import org.apache.fluss.predicate.{CompoundPredicate, LeafPredicate, Predicate, PredicateBuilder}
import org.apache.fluss.row.BinaryString
import org.apache.fluss.types.{DataField, DataTypes, RowType}

import org.apache.spark.sql.sources._
import org.assertj.core.api.Assertions.assertThat
import org.scalatest.funsuite.AnyFunSuite

import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime}
import java.util.Arrays

class SparkPredicateConverterTest extends AnyFunSuite {

  private val rowType: RowType = rowTypeOf(
    ("id", DataTypes.INT()),
    ("name", DataTypes.STRING()),
    ("score", DataTypes.DOUBLE()),
    ("active", DataTypes.BOOLEAN()),
    ("age", DataTypes.TINYINT()),
    ("balance", DataTypes.DECIMAL(10, 2)),
    ("dt", DataTypes.DATE()),
    ("ts", DataTypes.TIMESTAMP()),
    ("tsLtz", DataTypes.TIMESTAMP_LTZ())
  )

  test("EqualTo on integer column converts to equal predicate") {
    val predicate = convert(EqualTo("id", 42))
    val expected = new PredicateBuilder(rowType).equal(0, Integer.valueOf(42))
    assertThat(predicate).isEqualTo(expected)
  }

  test("EqualTo on string column wraps literal as BinaryString") {
    val predicate = convert(EqualTo("name", "alice"))
    val expected = new PredicateBuilder(rowType).equal(1, BinaryString.fromString("alice"))
    assertThat(predicate).isEqualTo(expected)
  }

  test("EqualTo on nullable literal returns null predicate value") {
    val predicate = convert(EqualTo("id", null))
    val expected = new PredicateBuilder(rowType).equal(0, null.asInstanceOf[Object])
    assertThat(predicate).isEqualTo(expected)
  }

  test("EqualNullSafe with null maps to isNull") {
    val predicate = convert(EqualNullSafe("id", null))
    val expected = new PredicateBuilder(rowType).isNull(0)
    assertThat(predicate).isEqualTo(expected)
  }

  test("EqualNullSafe with value maps to equal") {
    val predicate = convert(EqualNullSafe("id", 7))
    val expected = new PredicateBuilder(rowType).equal(0, Integer.valueOf(7))
    assertThat(predicate).isEqualTo(expected)
  }

  test("GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual") {
    val builder = new PredicateBuilder(rowType)
    assertThat(convert(GreaterThan("id", 10)))
      .isEqualTo(builder.greaterThan(0, Integer.valueOf(10)))
    assertThat(convert(GreaterThanOrEqual("id", 10)))
      .isEqualTo(builder.greaterOrEqual(0, Integer.valueOf(10)))
    assertThat(convert(LessThan("id", 10)))
      .isEqualTo(builder.lessThan(0, Integer.valueOf(10)))
    assertThat(convert(LessThanOrEqual("id", 10)))
      .isEqualTo(builder.lessOrEqual(0, Integer.valueOf(10)))
  }

  test("IsNull and IsNotNull") {
    val builder = new PredicateBuilder(rowType)
    assertThat(convert(IsNull("name"))).isEqualTo(builder.isNull(1))
    assertThat(convert(IsNotNull("name"))).isEqualTo(builder.isNotNull(1))
  }

  test("In with multiple string literals") {
    val predicate = convert(In("name", Array[Any]("a", "b", "c")))
    val expected = new PredicateBuilder(rowType).in(
      1,
      Arrays.asList[Object](
        BinaryString.fromString("a"),
        BinaryString.fromString("b"),
        BinaryString.fromString("c")))
    assertThat(predicate).isEqualTo(expected)
  }

  test("And composes children") {
    val predicate = convert(And(EqualTo("id", 1), EqualTo("name", "x")))
    val builder = new PredicateBuilder(rowType)
    val expected = PredicateBuilder.and(
      builder.equal(0, Integer.valueOf(1)),
      builder.equal(1, BinaryString.fromString("x")))
    assertThat(predicate).isEqualTo(expected)
  }

  test("Or composes children") {
    val predicate = convert(Or(EqualTo("id", 1), EqualTo("id", 2)))
    val builder = new PredicateBuilder(rowType)
    val expected = PredicateBuilder.or(
      builder.equal(0, Integer.valueOf(1)),
      builder.equal(0, Integer.valueOf(2)))
    assertThat(predicate).isEqualTo(expected)
  }

  test("Not wrapping IsNull maps to isNotNull") {
    assertThat(convert(Not(IsNull("name"))))
      .isEqualTo(new PredicateBuilder(rowType).isNotNull(1))
  }

  test("Not wrapping IsNotNull maps to isNull") {
    assertThat(convert(Not(IsNotNull("name"))))
      .isEqualTo(new PredicateBuilder(rowType).isNull(1))
  }

  test("Not wrapping a non-null-check is not supported") {
    assert(SparkPredicateConverter.convert(rowType, Not(EqualTo("id", 1))).isEmpty)
  }

  test("Not wrapping equality on a boolean column rewrites to its complement") {
    assertThat(convert(Not(EqualTo("active", java.lang.Boolean.TRUE))))
      .isEqualTo(new PredicateBuilder(rowType).equal(3, java.lang.Boolean.FALSE))
  }

  test("StringStartsWith / EndsWith / Contains on string column") {
    val builder = new PredicateBuilder(rowType)
    assertThat(convert(StringStartsWith("name", "ali")))
      .isEqualTo(builder.startsWith(1, BinaryString.fromString("ali")))
    assertThat(convert(StringEndsWith("name", "son")))
      .isEqualTo(builder.endsWith(1, BinaryString.fromString("son")))
    assertThat(convert(StringContains("name", "li")))
      .isEqualTo(builder.contains(1, BinaryString.fromString("li")))
  }

  test("StringStartsWith rejected on non-string column") {
    assert(SparkPredicateConverter.convert(rowType, StringStartsWith("id", "1")).isEmpty)
  }

  test("Boolean literal") {
    val predicate = convert(EqualTo("active", java.lang.Boolean.TRUE))
    val expected = new PredicateBuilder(rowType).equal(3, java.lang.Boolean.TRUE)
    assertThat(predicate).isEqualTo(expected)
  }

  test("Tinyint literal from Integer auto-narrows") {
    val predicate = convert(EqualTo("age", Integer.valueOf(25)))
    val expected = new PredicateBuilder(rowType).equal(4, java.lang.Byte.valueOf(25.toByte))
    assertThat(predicate).isEqualTo(expected)
  }

  test("Decimal literal") {
    val bd = new JBigDecimal("123.45")
    val predicate = convert(EqualTo("balance", bd))
    assertThat(predicate).isNotNull
    assertThat(predicate.isInstanceOf[LeafPredicate]).isTrue
  }

  test("Date literal from java.sql.Date and LocalDate") {
    val daySql = Date.valueOf("2025-01-15")
    val predSql = convert(EqualTo("dt", daySql))
    val expected = new PredicateBuilder(rowType).equal(6, LocalDate.of(2025, 1, 15))
    assertThat(predSql).isEqualTo(expected)

    val predLocal = convert(EqualTo("dt", LocalDate.of(2025, 1, 15)))
    assertThat(predLocal).isEqualTo(expected)
  }

  test("Timestamp literal from java.sql.Timestamp") {
    val ts = Timestamp.valueOf("2025-12-31 10:00:00")
    val predicate = convert(EqualTo("ts", ts))
    assertThat(predicate).isNotNull
  }

  test("LocalDateTime literal") {
    val ldt = LocalDateTime.of(2025, 12, 31, 10, 0)
    val predicate = convert(EqualTo("ts", ldt))
    assertThat(predicate).isNotNull
  }

  test("Timestamp with local time zone from Instant") {
    val inst = Instant.parse("2025-12-31T10:00:00Z")
    val predicate = convert(EqualTo("tsLtz", inst))
    assertThat(predicate).isNotNull
  }

  test("Unknown column returns None") {
    assert(SparkPredicateConverter.convert(rowType, EqualTo("missing", 1)).isEmpty)
  }

  test("AlwaysTrue / AlwaysFalse return None (unsupported)") {
    assert(SparkPredicateConverter.convert(rowType, AlwaysTrue).isEmpty)
    assert(SparkPredicateConverter.convert(rowType, AlwaysFalse).isEmpty)
  }

  test("convertFilters returns AND of all convertible and the accepted list") {
    val filters: Seq[Filter] =
      Seq(EqualTo("id", 1), IsNotNull("name"), EqualTo("unknown", 1))

    val (predicate, accepted) = SparkPredicateConverter.convertFilters(rowType, filters)

    assert(predicate.isDefined)
    assert(predicate.get.isInstanceOf[CompoundPredicate])
    assert(accepted == Seq(filters(0), filters(1)))
  }

  test("convertFilters collapses a single filter without wrapping in AND") {
    val filters: Seq[Filter] = Seq(EqualTo("id", 1))
    val (predicate, accepted) = SparkPredicateConverter.convertFilters(rowType, filters)
    assert(predicate.isDefined)
    assert(predicate.get.isInstanceOf[LeafPredicate])
    assert(accepted == Seq(filters.head))
  }

  test("convertFilters with no convertible filters returns empty") {
    val filters: Seq[Filter] = Seq(EqualTo("unknown", 1), AlwaysTrue)
    val (predicate, accepted) = SparkPredicateConverter.convertFilters(rowType, filters)
    assert(predicate.isEmpty)
    assert(accepted.isEmpty)
  }

  private def convert(filter: Filter): Predicate =
    SparkPredicateConverter
      .convert(rowType, filter)
      .getOrElse(fail(s"Expected filter $filter to be convertible"))

  private def rowTypeOf(fields: (String, org.apache.fluss.types.DataType)*): RowType = {
    val list = new java.util.ArrayList[DataField]()
    fields.foreach {
      case (name, tpe) =>
        list.add(new DataField(name, tpe))
    }
    new RowType(list)
  }
}
