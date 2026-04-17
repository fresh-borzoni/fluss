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

import org.apache.fluss.predicate.{Predicate, PredicateBuilder, UnsupportedExpression}
import org.apache.fluss.row.{BinaryString, Decimal, TimestampLtz, TimestampNtz}
import org.apache.fluss.types._

import org.apache.spark.sql.sources._

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date => SqlDate, Timestamp => SqlTimestamp}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}

import scala.collection.JavaConverters._

/**
 * Converts Spark [[Filter]]s into Fluss [[Predicate]]s. Unsupported filters yield `None`; the
 * caller is expected to re-apply all original filters (Fluss pushdown is record-batch granularity
 * only, not row-exact).
 */
object SparkPredicateConverter {

  def convert(rowType: RowType, filter: Filter): Option[Predicate] = {
    val builder = new PredicateBuilder(rowType)
    try {
      Some(toPredicate(rowType, builder, filter))
    } catch {
      case _: UnsupportedExpression => None
    }
  }

  /** Returns the AND of all convertible filters, plus the subset of Spark filters accepted. */
  def convertFilters(rowType: RowType, filters: Seq[Filter]): (Option[Predicate], Seq[Filter]) = {
    val accepted = Seq.newBuilder[Filter]
    val predicates = Seq.newBuilder[Predicate]
    filters.foreach {
      f =>
        convert(rowType, f) match {
          case Some(p) =>
            accepted += f
            predicates += p
          case None =>
        }
    }
    val predicateList = predicates.result()
    val combined = predicateList.size match {
      case 0 => None
      case 1 => Some(predicateList.head)
      case _ => Some(PredicateBuilder.and(predicateList.asJava))
    }
    (combined, accepted.result())
  }

  private def toPredicate(rowType: RowType, builder: PredicateBuilder, filter: Filter): Predicate =
    filter match {
      case And(left, right) =>
        PredicateBuilder.and(
          toPredicate(rowType, builder, left),
          toPredicate(rowType, builder, right))

      case Or(left, right) =>
        PredicateBuilder.or(
          toPredicate(rowType, builder, left),
          toPredicate(rowType, builder, right))

      case Not(child) =>
        // PredicateBuilder has no generic negate; handle only shapes we can invert by hand.
        child match {
          case IsNull(attr) => builder.isNotNull(indexOf(builder, attr))
          case IsNotNull(attr) => builder.isNull(indexOf(builder, attr))
          case EqualTo(attr, value) if isBoolean(rowType, attr) && value.isInstanceOf[Boolean] =>
            builder.equal(indexOf(builder, attr), JBoolean.valueOf(!value.asInstanceOf[Boolean]))
          case _ => throw new UnsupportedExpression
        }

      case EqualTo(attr, value) =>
        val idx = indexOf(builder, attr)
        builder.equal(idx, literal(rowType.getTypeAt(idx), value))

      case EqualNullSafe(attr, value) if value == null =>
        builder.isNull(indexOf(builder, attr))

      case EqualNullSafe(attr, value) =>
        val idx = indexOf(builder, attr)
        builder.equal(idx, literal(rowType.getTypeAt(idx), value))

      case GreaterThan(attr, value) =>
        val idx = indexOf(builder, attr)
        builder.greaterThan(idx, literal(rowType.getTypeAt(idx), value))

      case GreaterThanOrEqual(attr, value) =>
        val idx = indexOf(builder, attr)
        builder.greaterOrEqual(idx, literal(rowType.getTypeAt(idx), value))

      case LessThan(attr, value) =>
        val idx = indexOf(builder, attr)
        builder.lessThan(idx, literal(rowType.getTypeAt(idx), value))

      case LessThanOrEqual(attr, value) =>
        val idx = indexOf(builder, attr)
        builder.lessOrEqual(idx, literal(rowType.getTypeAt(idx), value))

      case IsNull(attr) =>
        builder.isNull(indexOf(builder, attr))

      case IsNotNull(attr) =>
        builder.isNotNull(indexOf(builder, attr))

      case In(attr, values) =>
        val idx = indexOf(builder, attr)
        val fieldType = rowType.getTypeAt(idx)
        val literals = values.toSeq.map(literal(fieldType, _))
        builder.in(idx, literals.asJava)

      case StringStartsWith(attr, prefix) =>
        val idx = indexOf(builder, attr)
        requireStringType(rowType.getTypeAt(idx))
        builder.startsWith(idx, BinaryString.fromString(prefix))

      case StringEndsWith(attr, suffix) =>
        val idx = indexOf(builder, attr)
        requireStringType(rowType.getTypeAt(idx))
        builder.endsWith(idx, BinaryString.fromString(suffix))

      case StringContains(attr, substr) =>
        val idx = indexOf(builder, attr)
        requireStringType(rowType.getTypeAt(idx))
        builder.contains(idx, BinaryString.fromString(substr))

      case _ => throw new UnsupportedExpression
    }

  private def indexOf(builder: PredicateBuilder, fieldName: String): Int = {
    val idx = builder.indexOf(fieldName)
    if (idx < 0) throw new UnsupportedExpression
    idx
  }

  private def requireStringType(tpe: DataType): Unit = tpe.getTypeRoot match {
    case DataTypeRoot.STRING | DataTypeRoot.CHAR =>
    case _ => throw new UnsupportedExpression
  }

  private def isBoolean(rowType: RowType, fieldName: String): Boolean = {
    val idx = rowType.getFieldNames.indexOf(fieldName)
    idx >= 0 && rowType.getTypeAt(idx).getTypeRoot == DataTypeRoot.BOOLEAN
  }

  // Spark DSv2 Filter values are raw Java/Scala objects (not Catalyst internal types).
  private def literal(tpe: DataType, value: Any): Object = {
    if (value == null) return null
    tpe.getTypeRoot match {
      case DataTypeRoot.BOOLEAN =>
        value match {
          case b: JBoolean => b
          case _ => throw new UnsupportedExpression
        }

      case DataTypeRoot.TINYINT =>
        value match {
          case n: Number => JByte.valueOf(n.byteValue())
          case _ => throw new UnsupportedExpression
        }

      case DataTypeRoot.SMALLINT =>
        value match {
          case n: Number => JShort.valueOf(n.shortValue())
          case _ => throw new UnsupportedExpression
        }

      case DataTypeRoot.INTEGER =>
        value match {
          case n: Number => Integer.valueOf(n.intValue())
          case _ => throw new UnsupportedExpression
        }

      case DataTypeRoot.BIGINT =>
        value match {
          case n: Number => JLong.valueOf(n.longValue())
          case _ => throw new UnsupportedExpression
        }

      case DataTypeRoot.FLOAT =>
        value match {
          case n: Number => JFloat.valueOf(n.floatValue())
          case _ => throw new UnsupportedExpression
        }

      case DataTypeRoot.DOUBLE =>
        value match {
          case n: Number => JDouble.valueOf(n.doubleValue())
          case _ => throw new UnsupportedExpression
        }

      case DataTypeRoot.STRING | DataTypeRoot.CHAR =>
        value match {
          case s: String => BinaryString.fromString(s)
          case _ => throw new UnsupportedExpression
        }

      case DataTypeRoot.BINARY | DataTypeRoot.BYTES =>
        value match {
          case b: Array[Byte] => b
          case _ => throw new UnsupportedExpression
        }

      case DataTypeRoot.DECIMAL =>
        val dt = tpe.asInstanceOf[DecimalType]
        value match {
          case bd: JBigDecimal => Decimal.fromBigDecimal(bd, dt.getPrecision, dt.getScale)
          case bd: scala.math.BigDecimal =>
            Decimal.fromBigDecimal(bd.bigDecimal, dt.getPrecision, dt.getScale)
          case _ => throw new UnsupportedExpression
        }

      case DataTypeRoot.DATE =>
        // RPC serialization (PredicateMessageUtils) expects LocalDate.
        value match {
          case d: LocalDate => d
          case d: SqlDate => d.toLocalDate
          case i: Integer => LocalDate.ofEpochDay(i.longValue())
          case _ => throw new UnsupportedExpression
        }

      case DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE =>
        value match {
          case ts: SqlTimestamp =>
            TimestampNtz.fromMillis(ts.getTime, ts.getNanos % 1000000)
          case ldt: LocalDateTime =>
            TimestampNtz.fromLocalDateTime(ldt)
          case _ => throw new UnsupportedExpression
        }

      case DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        value match {
          case ts: SqlTimestamp =>
            TimestampLtz.fromEpochMillis(ts.getTime, ts.getNanos % 1000000)
          case inst: Instant =>
            TimestampLtz.fromInstant(inst)
          case ldt: LocalDateTime =>
            TimestampLtz.fromInstant(ldt.toInstant(ZoneOffset.UTC))
          case _ => throw new UnsupportedExpression
        }

      case _ => throw new UnsupportedExpression
    }
  }
}
