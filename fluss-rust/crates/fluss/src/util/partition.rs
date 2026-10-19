// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/// Utils for partition.
use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::{AutoPartitionStrategy, DataType};
use crate::row::{Date, Datum, Time, TimestampLtz, TimestampNtz};
use jiff::ToSpan;
use std::fmt::Write;

/// The partition value of the system partition that answers lookups of expired partitions from
/// the lake.
pub const HISTORICAL_PARTITION_VALUE: &str = "__historical__";

/// A field of an auto-partition time pattern.
#[derive(Clone, Copy, Debug, PartialEq)]
enum TimeField {
    Year,
    Quarter,
    Month,
    Day,
    Hour,
    Literal(char),
}

impl TimeField {
    /// The digits of a numeric field and the values they may take.
    fn digits(self) -> Option<(usize, u32, u32)> {
        match self {
            TimeField::Year => Some((4, 0, 9999)),
            TimeField::Quarter => Some((1, 1, 4)),
            TimeField::Month => Some((2, 1, 12)),
            TimeField::Day => Some((2, 1, 31)),
            TimeField::Hour => Some((2, 0, 23)),
            TimeField::Literal(_) => None,
        }
    }
}

/// Parses the `DateTimeFormatter` patterns auto-partitioning uses: the fixed-width fields `yyyy`,
/// `Q`, `MM`, `dd` and `HH`, and separators that are not letters. Returns `None` for anything
/// else, so such a table never treats a partition as past.
fn parse_time_pattern(pattern: &str) -> Option<Vec<TimeField>> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut fields = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let run = chars[i..].iter().take_while(|&&d| d == c).count();
        match (c, run) {
            ('y', 4) => fields.push(TimeField::Year),
            ('Q', 1) => fields.push(TimeField::Quarter),
            ('M', 2) => fields.push(TimeField::Month),
            ('d', 2) => fields.push(TimeField::Day),
            ('H', 2) => fields.push(TimeField::Hour),
            (c, _) if c.is_ascii_alphabetic() || c == '\'' => return None,
            (c, run) => fields.extend(std::iter::repeat_n(TimeField::Literal(c), run)),
        }
        i += run;
    }
    Some(fields)
}

fn matches_time_pattern(value: &str, fields: &[TimeField]) -> bool {
    let mut rest = value;
    for field in fields {
        match (field, field.digits()) {
            (TimeField::Literal(c), _) => match rest.strip_prefix(*c) {
                Some(after) => rest = after,
                None => return false,
            },
            (_, Some((width, min, max))) => {
                let Some(digits) = rest.get(..width) else {
                    return false;
                };
                if !digits.bytes().all(|b| b.is_ascii_digit()) {
                    return false;
                }
                let n: u32 = digits.parse().unwrap_or(u32::MAX);
                if n < min || n > max {
                    return false;
                }
                rest = &rest[width..];
            }
            (_, None) => return false,
        }
    }
    rest.is_empty()
}

fn format_time(time: &jiff::Zoned, fields: &[TimeField]) -> String {
    let mut out = String::new();
    for field in fields {
        let _ = match field {
            TimeField::Year => write!(out, "{:04}", time.year()),
            TimeField::Quarter => write!(out, "{}", (time.month() - 1) / 3 + 1),
            TimeField::Month => write!(out, "{:02}", time.month()),
            TimeField::Day => write!(out, "{:02}", time.day()),
            TimeField::Hour => write!(out, "{:02}", time.hour()),
            TimeField::Literal(c) => write!(out, "{c}"),
        };
    }
    out
}

fn auto_partition_time_zone(name: &str) -> Option<jiff::tz::TimeZone> {
    if ["UTC", "Z", "GMT"]
        .iter()
        .any(|utc| name.eq_ignore_ascii_case(utc))
    {
        return Some(jiff::tz::TimeZone::UTC);
    }
    jiff::tz::TimeZone::get(name).ok()
}

/// Whether `partition_time` is a valid auto-partition value earlier than the partition holding
/// `now`, as Java's `PartitionUtils.isPastAutoPartition` decides it: the current partition and
/// later ones may still be created, so only an earlier one is known to have expired.
pub fn is_past_auto_partition(
    partition_time: &str,
    strategy: &AutoPartitionStrategy,
    now: jiff::Timestamp,
) -> bool {
    let pattern = match strategy.time_format() {
        Some(format) => format,
        None => match strategy.time_unit().to_ascii_uppercase().as_str() {
            "YEAR" => "yyyy",
            "QUARTER" => "yyyyQ",
            "MONTH" => "yyyyMM",
            "DAY" => "yyyyMMdd",
            "HOUR" => "yyyyMMddHH",
            _ => return false,
        },
    };
    let Some(fields) = parse_time_pattern(pattern) else {
        return false;
    };
    if !matches_time_pattern(partition_time, &fields) {
        return false;
    }
    let Some(time_zone) = auto_partition_time_zone(strategy.timezone()) else {
        return false;
    };
    let current = format_time(&now.to_zoned(time_zone), &fields);
    partition_time < current.as_str()
}

fn hex_string(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        write!(hex, "{b:02x}").unwrap();
    }
    hex
}

fn reformat_float(value: f32) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value > 0.0 {
            "Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        value.to_string().replace('.', "_")
    }
}

fn reformat_double(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value > 0.0 {
            "Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        value.to_string().replace('.', "_")
    }
}

const UNIX_EPOCH_DATE: jiff::civil::Date = jiff::civil::date(1970, 1, 1);

fn day_to_string(days: i32) -> String {
    let date = UNIX_EPOCH_DATE + days.days();
    format!("{:04}-{:02}-{:02}", date.year(), date.month(), date.day())
}

fn date_to_string(date: Date) -> String {
    day_to_string(date.get_inner())
}

const MILLIS_PER_SECOND: i64 = 1_000;
const MILLIS_PER_MINUTE: i64 = 60 * MILLIS_PER_SECOND;
const MILLIS_PER_HOUR: i64 = 60 * MILLIS_PER_MINUTE;

fn milli_to_string(milli: i32) -> String {
    let hour = milli.div_euclid(MILLIS_PER_HOUR as i32);
    let min = milli
        .rem_euclid(MILLIS_PER_HOUR as i32)
        .div_euclid(MILLIS_PER_MINUTE as i32);
    let sec = milli
        .rem_euclid(MILLIS_PER_MINUTE as i32)
        .div_euclid(MILLIS_PER_SECOND as i32);
    let ms = milli.rem_euclid(MILLIS_PER_SECOND as i32);

    format!("{hour:02}-{min:02}-{sec:02}_{ms:03}")
}

fn time_to_string(time: Time) -> String {
    milli_to_string(time.get_inner())
}

trait Timestamp {
    fn get_milli(&self) -> i64;
    fn get_nano_of_milli(&self) -> i32;
}

impl Timestamp for TimestampNtz {
    fn get_milli(&self) -> i64 {
        self.get_millisecond()
    }

    fn get_nano_of_milli(&self) -> i32 {
        self.get_nano_of_millisecond()
    }
}

impl Timestamp for TimestampLtz {
    fn get_milli(&self) -> i64 {
        self.get_epoch_millisecond()
    }

    fn get_nano_of_milli(&self) -> i32 {
        self.get_nano_of_millisecond()
    }
}

/// This formats date time while adhering to java side behaviour
///
fn timestamp_to_string<T: Timestamp>(ts: T) -> String {
    let millis = ts.get_milli();
    let nanos = ts.get_nano_of_milli();

    let millis_of_second = millis.rem_euclid(MILLIS_PER_SECOND);
    let total_secs = millis.div_euclid(MILLIS_PER_SECOND);

    let epoch = jiff::Timestamp::UNIX_EPOCH;
    let ts_jiff = epoch + jiff::Span::new().seconds(total_secs);
    let dt = ts_jiff.to_zoned(jiff::tz::TimeZone::UTC).datetime();

    if nanos > 0 {
        format!(
            "{:04}-{:02}-{:02}-{:02}-{:02}-{:02}_{:03}{:06}",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second(),
            millis_of_second,
            nanos
        )
    } else if millis_of_second > 0 {
        format!(
            "{:04}-{:02}-{:02}-{:02}-{:02}-{:02}_{:03}",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second(),
            millis_of_second
        )
    } else {
        format!(
            "{:04}-{:02}-{:02}-{:02}-{:02}-{:02}_",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second(),
        )
    }
}

/// Converts a Datum value to its string representation for partition naming.
pub fn convert_value_of_type(value: &Datum, data_type: &DataType) -> Result<String> {
    match (value, data_type) {
        (Datum::String(s), DataType::Char(_) | DataType::String(_)) => Ok(s.to_string()),
        (Datum::Bool(b), DataType::Boolean(_)) => Ok(b.to_string()),
        (Datum::Blob(bytes), DataType::Binary(_) | DataType::Bytes(_)) => Ok(hex_string(bytes)),
        (Datum::Int8(v), DataType::TinyInt(_)) => Ok(v.to_string()),
        (Datum::Int16(v), DataType::SmallInt(_)) => Ok(v.to_string()),
        (Datum::Int32(v), DataType::Int(_)) => Ok(v.to_string()),
        (Datum::Int64(v), DataType::BigInt(_)) => Ok(v.to_string()),
        (Datum::Date(d), DataType::Date(_)) => Ok(date_to_string(*d)),
        (Datum::Time(t), DataType::Time(_)) => Ok(time_to_string(*t)),
        (Datum::Float32(f), DataType::Float(_)) => Ok(reformat_float(f.into_inner())),
        (Datum::Float64(f), DataType::Double(_)) => Ok(reformat_double(f.into_inner())),
        (Datum::TimestampLtz(ts), DataType::TimestampLTz(_)) => Ok(timestamp_to_string(*ts)),
        (Datum::TimestampNtz(ts), DataType::Timestamp(_)) => Ok(timestamp_to_string(*ts)),
        _ => Err(IllegalArgument {
            message: format!(
                "Unsupported conversion to partition key from data type: {data_type:?}, value: {value:?}"
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    fn strategy(properties: &[(&str, &str)]) -> AutoPartitionStrategy {
        let mut map: HashMap<String, String> = properties
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        map.entry("table.auto-partition.time-zone".to_string())
            .or_insert_with(|| "UTC".to_string());
        AutoPartitionStrategy::from(&map)
    }

    fn at(time: &str) -> jiff::Timestamp {
        time.parse().unwrap()
    }

    #[test]
    fn only_a_partition_before_the_current_one_is_past() {
        let hourly = strategy(&[("table.auto-partition.time-unit", "HOUR")]);
        let now = at("2026-10-17T12:30:00Z");
        assert!(is_past_auto_partition("2026101711", &hourly, now));
        assert!(is_past_auto_partition("2025010100", &hourly, now));
        assert!(!is_past_auto_partition("2026101712", &hourly, now));
        assert!(!is_past_auto_partition("2026101713", &hourly, now));
    }

    #[test]
    fn a_value_the_pattern_does_not_parse_is_never_past() {
        let daily = strategy(&[]);
        let now = at("2026-10-17T12:30:00Z");
        assert!(is_past_auto_partition("20261016", &daily, now));
        assert!(!is_past_auto_partition("2026-10-16", &daily, now));
        assert!(!is_past_auto_partition("20261316", &daily, now));
        assert!(!is_past_auto_partition("2026101", &daily, now));
        assert!(!is_past_auto_partition("__historical__", &daily, now));
    }

    #[test]
    fn the_current_partition_is_taken_in_the_table_time_zone() {
        let daily = strategy(&[("table.auto-partition.time-zone", "Asia/Shanghai")]);
        // 17 Oct 20:00 UTC is already 18 Oct in Shanghai.
        let now = at("2026-10-17T20:00:00Z");
        assert!(is_past_auto_partition("20261017", &daily, now));
        assert!(!is_past_auto_partition("20261018", &daily, now));
    }

    #[test]
    fn a_custom_time_format_and_quarters() {
        let custom = strategy(&[
            ("table.auto-partition.time-unit", "DAY"),
            ("table.auto-partition.time-format", "yyyy-MM-dd"),
        ]);
        let quarterly = strategy(&[("table.auto-partition.time-unit", "QUARTER")]);
        let now = at("2026-10-17T12:30:00Z");
        assert!(is_past_auto_partition("2026-10-16", &custom, now));
        assert!(!is_past_auto_partition("2026-10-17", &custom, now));
        assert!(!is_past_auto_partition("20261016", &custom, now));
        assert!(is_past_auto_partition("20263", &quarterly, now));
        assert!(!is_past_auto_partition("20264", &quarterly, now));
    }

    #[test]
    fn an_unsupported_pattern_never_falls_back() {
        let named_month = strategy(&[("table.auto-partition.time-format", "yyyy-MMM")]);
        assert!(!is_past_auto_partition(
            "2020-Jan",
            &named_month,
            at("2026-10-17T12:30:00Z")
        ));
    }

    use super::*;
    use crate::metadata::{
        BigIntType, BinaryType, BooleanType, BytesType, CharType, DateType, DoubleType, FloatType,
        IntType, SmallIntType, StringType, TimeType, TimestampLTzType, TimestampType, TinyIntType,
    };
    use crate::row::{Date, Time, TimestampLtz, TimestampNtz};
    use std::borrow::Cow;

    use crate::metadata::TablePath;

    #[test]
    fn test_string() {
        let datum = Datum::String(Cow::Borrowed("Fluss"));

        let to_string_result = convert_value_of_type(&datum, &DataType::String(StringType::new()))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "Fluss");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_char() {
        let datum = Datum::String(Cow::Borrowed("F"));

        let to_string_result = convert_value_of_type(&datum, &DataType::Char(CharType::new(1)))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "F");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_boolean() {
        let datum = Datum::Bool(true);

        let to_string_result =
            convert_value_of_type(&datum, &DataType::Boolean(BooleanType::new()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "true");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_byte() {
        let datum = Datum::Blob(Cow::Borrowed(&[0x10, 0x20, 0x30, 0x40, 0x50, 0xFF]));

        let to_string_result = convert_value_of_type(&datum, &DataType::Bytes(BytesType::new()))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "1020304050ff");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_binary() {
        let datum = Datum::Blob(Cow::Borrowed(&[0x10, 0x20, 0x30, 0x40, 0x50, 0xFF]));

        let to_string_result = convert_value_of_type(&datum, &DataType::Binary(BinaryType::new(6)))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "1020304050ff");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_tiny_int() {
        let datum = Datum::Int8(100);

        let to_string_result =
            convert_value_of_type(&datum, &DataType::TinyInt(TinyIntType::new()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "100");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_small_int() {
        let datum = Datum::Int16(-32760);

        let to_string_result =
            convert_value_of_type(&datum, &DataType::SmallInt(SmallIntType::new()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "-32760");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_int() {
        let datum = Datum::Int32(299000);

        let to_string_result = convert_value_of_type(&datum, &DataType::Int(IntType::new()))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "299000");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_big_int() {
        let datum = Datum::Int64(1748662955428);

        let to_string_result = convert_value_of_type(&datum, &DataType::BigInt(BigIntType::new()))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "1748662955428");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_date() {
        let datum = Datum::Date(Date::new(20235));

        let to_string_result = convert_value_of_type(&datum, &DataType::Date(DateType::new()))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-27");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_time() {
        let datum = Datum::Time(Time::new(5402199));

        let to_string_result =
            convert_value_of_type(&datum, &DataType::Time(TimeType::new(3).unwrap()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "01-30-02_199");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_float() {
        let datum = Datum::Float32(5.73.into());

        let to_string_result = convert_value_of_type(&datum, &DataType::Float(FloatType::new()))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "5_73");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        let datum = Datum::Float32(f32::NAN.into());
        assert_eq!(
            convert_value_of_type(&datum, &DataType::Float(FloatType::new()))
                .expect("datum conversion to partition string failed"),
            "NaN"
        );

        let datum = Datum::Float32(f32::INFINITY.into());
        assert_eq!(
            convert_value_of_type(&datum, &DataType::Float(FloatType::new()))
                .expect("datum conversion to partition string failed"),
            "Inf"
        );

        let datum = Datum::Float32(f32::NEG_INFINITY.into());
        assert_eq!(
            convert_value_of_type(&datum, &DataType::Float(FloatType::new()))
                .expect("datum conversion to partition string failed"),
            "-Inf"
        );
    }

    #[test]
    fn test_double() {
        let datum = Datum::Float64(5.73737.into());

        let to_string_result = convert_value_of_type(&datum, &DataType::Double(DoubleType::new()))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "5_73737");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        let datum = Datum::Float64(f64::NAN.into());
        assert_eq!(
            convert_value_of_type(&datum, &DataType::Double(DoubleType::new()))
                .expect("datum conversion to partition string failed"),
            "NaN"
        );

        let datum = Datum::Float64(f64::INFINITY.into());
        assert_eq!(
            convert_value_of_type(&datum, &DataType::Double(DoubleType::new()))
                .expect("datum conversion to partition string failed"),
            "Inf"
        );

        let datum = Datum::Float64(f64::NEG_INFINITY.into());
        assert_eq!(
            convert_value_of_type(&datum, &DataType::Double(DoubleType::new()))
                .expect("datum conversion to partition string failed"),
            "-Inf"
        );
    }

    #[test]
    fn test_timestamp_ntz() {
        let datum = Datum::TimestampNtz(
            TimestampNtz::from_millis_nanos(1748662955428, 99988)
                .expect("TimestampNtz init failed"),
        );

        let to_string_result =
            convert_value_of_type(&datum, &DataType::Timestamp(TimestampType::new(9).unwrap()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-31-03-42-35_428099988");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        // Zero nanos of millis
        let datum = Datum::TimestampNtz(
            TimestampNtz::from_millis_nanos(1748662955428, 0).expect("TimestampNtz init failed"),
        );

        let to_string_result =
            convert_value_of_type(&datum, &DataType::Timestamp(TimestampType::new(9).unwrap()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-31-03-42-35_428");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        // Zero millis
        let datum = Datum::TimestampNtz(
            TimestampNtz::from_millis_nanos(1748662955000, 99988)
                .expect("TimestampNtz init failed"),
        );

        let to_string_result =
            convert_value_of_type(&datum, &DataType::Timestamp(TimestampType::new(9).unwrap()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-31-03-42-35_000099988");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        // Zero millis and zero nanos
        let datum = Datum::TimestampNtz(
            TimestampNtz::from_millis_nanos(1748662955000, 0).expect("TimestampNtz init failed"),
        );

        let to_string_result =
            convert_value_of_type(&datum, &DataType::Timestamp(TimestampType::new(9).unwrap()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-31-03-42-35_");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        // Negative millis
        let datum = Datum::TimestampNtz(
            TimestampNtz::from_millis_nanos(-1748662955428, 99988)
                .expect("TimestampNtz init failed"),
        );

        let to_string_result =
            convert_value_of_type(&datum, &DataType::Timestamp(TimestampType::new(9).unwrap()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "1914-08-03-20-17-24_572099988");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_timestamp_ltz() {
        let datum = Datum::TimestampLtz(
            TimestampLtz::from_millis_nanos(1748662955428, 99988)
                .expect("TimestampLtz init failed"),
        );

        let to_string_result = convert_value_of_type(
            &datum,
            &DataType::TimestampLTz(TimestampLTzType::new(9).unwrap()),
        )
        .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-31-03-42-35_428099988");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        // Zero nanos of millis
        let datum = Datum::TimestampLtz(
            TimestampLtz::from_millis_nanos(1748662955428, 0).expect("TimestampLtz init failed"),
        );

        let to_string_result = convert_value_of_type(
            &datum,
            &DataType::TimestampLTz(TimestampLTzType::new(9).unwrap()),
        )
        .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-31-03-42-35_428");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        // Zero millis
        let datum = Datum::TimestampLtz(
            TimestampLtz::from_millis_nanos(1748662955000, 99988)
                .expect("TimestampLtz init failed"),
        );

        let to_string_result = convert_value_of_type(
            &datum,
            &DataType::TimestampLTz(TimestampLTzType::new(9).unwrap()),
        )
        .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-31-03-42-35_000099988");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        // Zero millis and zero nanos
        let datum = Datum::TimestampLtz(
            TimestampLtz::from_millis_nanos(1748662955000, 0).expect("TimestampLtz init failed"),
        );

        let to_string_result = convert_value_of_type(
            &datum,
            &DataType::TimestampLTz(TimestampLTzType::new(9).unwrap()),
        )
        .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-31-03-42-35_");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        // Negative millis
        let datum = Datum::TimestampLtz(
            TimestampLtz::from_millis_nanos(-1748662955428, 99988)
                .expect("TimestampLtz init failed"),
        );

        let to_string_result = convert_value_of_type(
            &datum,
            &DataType::TimestampLTz(TimestampLTzType::new(9).unwrap()),
        )
        .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "1914-08-03-20-17-24_572099988");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }
}
