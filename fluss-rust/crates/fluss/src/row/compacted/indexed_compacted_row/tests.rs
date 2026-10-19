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

use super::*;
use crate::metadata::{DataTypes, DecimalType, TimestampLTzType, TimestampType};
use crate::record::RowAppendRecordBatchBuilder;
use crate::row::CompactedRow;
use crate::row::binary::BinaryWriter;
use crate::row::binary_array::FlussArrayWriter;
use crate::row::compacted::compacted_row_writer::CompactedRowWriter;
use bigdecimal::{BigDecimal, num_bigint::BigInt};

fn row_type() -> RowType {
    RowType::with_data_types(vec![
        DataTypes::boolean(),
        DataTypes::tinyint(),
        DataTypes::smallint(),
        DataTypes::int(),
        DataTypes::bigint(),
        DataTypes::float(),
        DataTypes::double(),
        DataTypes::string(),
        DataTypes::bytes(),
        DataTypes::date(),
        DataTypes::time(),
        DataType::Timestamp(TimestampType::with_nullable(true, 3).unwrap()),
        DataType::Timestamp(TimestampType::with_nullable(true, 6).unwrap()),
        DataType::TimestampLTz(TimestampLTzType::with_nullable(true, 3).unwrap()),
        DataType::TimestampLTz(TimestampLTzType::with_nullable(true, 9).unwrap()),
        DataType::Decimal(DecimalType::new(10, 2).unwrap()),
        DataType::Decimal(DecimalType::new(28, 10).unwrap()),
        DataTypes::array(DataTypes::string()),
        DataTypes::string(),
    ])
}

/// Row `i`: every field set, except that every third row has nulls in the middle and at the end.
fn encode(row_type: &RowType, i: i64) -> Vec<u8> {
    let mut writer = CompactedRowWriter::new(row_type.fields().len());
    let nulls = i % 3 == 0;
    writer.write_boolean(i % 2 == 0);
    writer.write_byte(i as u8);
    writer.write_short(i as i16 * -3);
    writer.write_int(i as i32 * 1000);
    writer.write_long(i * 1_000_000_007);
    writer.write_float(i as f32 / 4.0);
    writer.write_double(i as f64 / 8.0);
    if nulls {
        writer.set_null_at(7);
    } else {
        writer.write_string(&format!("row-{i}-ü"));
    }
    writer.write_bytes(&i.to_le_bytes());
    writer.write_int(19651 + i as i32);
    writer.write_time(34_200_000, 0);
    writer.write_timestamp_ntz(&TimestampNtz::new(1_698_235_273_182 + i), 3);
    writer.write_timestamp_ntz(
        &TimestampNtz::from_millis_nanos(1_698_235_273_182, i as i32 % 1000).unwrap(),
        6,
    );
    writer.write_timestamp_ltz(&TimestampLtz::new(1_698_235_273_182 - i), 3);
    writer.write_timestamp_ltz(
        &TimestampLtz::from_millis_nanos(1_698_235_273_182, 999_999).unwrap(),
        9,
    );
    writer.write_decimal(
        &Decimal::from_big_decimal(BigDecimal::new(BigInt::from(12_345 + i), 2), 10, 2).unwrap(),
        10,
    );
    writer.write_decimal(
        &Decimal::from_big_decimal(
            BigDecimal::new(BigInt::from(999_999_999_999_999_999i128 - i as i128), 10),
            28,
            10,
        )
        .unwrap(),
        28,
    );
    if nulls {
        writer.set_null_at(17);
    } else {
        let element = DataTypes::string();
        let mut array = FlussArrayWriter::new(2, &element);
        array.write_string(0, "a");
        array.write_string(1, &format!("element-{i}"));
        writer.write_array(&array.complete().unwrap());
    }
    if nulls {
        writer.set_null_at(18);
    } else {
        writer.write_string("last");
    }
    writer.to_bytes().to_vec()
}

#[test]
fn builds_the_same_arrow_batch_as_a_decoded_row() {
    let row_type = row_type();
    let rows: Vec<Vec<u8>> = (0..10).map(|i| encode(&row_type, i)).collect();

    let mut decoded = RowAppendRecordBatchBuilder::new(&row_type).unwrap();
    for row in &rows {
        decoded
            .append(&CompactedRow::from_bytes(&row_type, row))
            .unwrap();
    }
    let mut in_place = RowAppendRecordBatchBuilder::new(&row_type).unwrap();
    let mut positions = Vec::new();
    for row in &rows {
        in_place
            .append(&IndexedCompactedRow::new(&row_type, row, &mut positions).unwrap())
            .unwrap();
    }
    let expected = decoded.build_arrow_record_batch().unwrap();
    let actual = in_place.build_arrow_record_batch().unwrap();
    assert_eq!(expected.num_rows(), 10);
    assert_eq!(actual, expected);
}

#[test]
fn reports_nulls_and_rejects_nested_rows() {
    let row_type = row_type();
    let bytes = encode(&row_type, 3);
    let mut positions = Vec::new();
    let row = IndexedCompactedRow::new(&row_type, &bytes, &mut positions).unwrap();
    assert!(row.is_null_at(7).unwrap());
    assert!(!row.is_null_at(8).unwrap());
    assert!(row.get_string(7).is_err());
    assert_eq!(row.get_int(3).unwrap(), 3000);

    let nested = RowType::with_data_types(vec![DataTypes::row(vec![DataTypes::field(
        "inner",
        DataTypes::int(),
    )])]);
    assert!(!IndexedCompactedRow::supports(&nested));
    assert!(IndexedCompactedRow::supports(&row_type));
}
