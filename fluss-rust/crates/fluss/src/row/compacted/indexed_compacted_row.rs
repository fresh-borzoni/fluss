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

//! A compacted row read in place, for scans that turn millions of rows into
//! Arrow columns.
//!
//! [`CompactedRow`](super::CompactedRow) decodes a whole row into a
//! [`GenericRow`](crate::row::GenericRow) on first access: a vector of datums
//! per row, plus a copy of every array or map. This row instead records where
//! each field starts, in a buffer the caller reuses from row to row, and every
//! getter reads its field at that position. Scalars and strings cost no
//! allocation; arrays and maps are still copied, because their views own their
//! bytes. Nested rows are not supported.

use crate::client::WriteFormat;
use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::{DataType, RowType};
use crate::row::binary_map::FlussMap;
use crate::row::compacted::compacted_row_reader::CompactedRowReader;
use crate::row::datum::{Date, Time, TimestampLtz, TimestampNtz};
use crate::row::view::{ArrayView, MapView};
use crate::row::{DataGetters, Decimal, FlussArray, InternalRow};

/// The position of a null field.
const NULL: usize = usize::MAX;

pub struct IndexedCompactedRow<'a> {
    row_type: &'a RowType,
    reader: CompactedRowReader<'a>,
    positions: &'a [usize],
}

impl<'a> IndexedCompactedRow<'a> {
    /// Whether rows of `row_type` can be read in place: every type but a nested row.
    pub fn supports(row_type: &RowType) -> bool {
        !row_type
            .fields()
            .iter()
            .any(|field| matches!(field.data_type, DataType::Row(_)))
    }

    /// Indexes `data`, one compacted row of `row_type`, into `positions`, which
    /// is cleared first so that one buffer serves a whole scan.
    pub fn new(
        row_type: &'a RowType,
        data: &'a [u8],
        positions: &'a mut Vec<usize>,
    ) -> Result<Self> {
        let fields = row_type.fields();
        let reader = CompactedRowReader::new(fields.len(), data, 0, data.len());
        positions.clear();
        let mut cursor = reader.initial_position();
        for (pos, field) in fields.iter().enumerate() {
            let data_type = &field.data_type;
            if data_type.is_nullable() && reader.is_null_at(pos) {
                positions.push(NULL);
                continue;
            }
            positions.push(cursor);
            cursor = match data_type {
                DataType::Boolean(_) | DataType::TinyInt(_) => reader.read_byte(cursor)?.1,
                DataType::SmallInt(_) => reader.read_short(cursor)?.1,
                DataType::Int(_) | DataType::Date(_) | DataType::Time(_) => {
                    reader.read_int(cursor)?.1
                }
                DataType::BigInt(_) => reader.read_long(cursor)?.1,
                DataType::Float(_) => reader.read_float(cursor)?.1,
                DataType::Double(_) => reader.read_double(cursor)?.1,
                DataType::Decimal(decimal)
                    if Decimal::is_compact_precision(decimal.precision()) =>
                {
                    reader.read_long(cursor)?.1
                }
                DataType::Timestamp(timestamp)
                    if TimestampNtz::is_compact(timestamp.precision()) =>
                {
                    reader.read_long(cursor)?.1
                }
                DataType::TimestampLTz(timestamp)
                    if TimestampLtz::is_compact(timestamp.precision()) =>
                {
                    reader.read_long(cursor)?.1
                }
                DataType::Timestamp(_) | DataType::TimestampLTz(_) => {
                    let (_, nanos) = reader.read_long(cursor)?;
                    reader.read_int(nanos)?.1
                }
                // Strings, binaries, wide decimals, arrays and maps are all length-prefixed bytes.
                DataType::Char(_)
                | DataType::String(_)
                | DataType::Bytes(_)
                | DataType::Binary(_)
                | DataType::Decimal(_)
                | DataType::Array(_)
                | DataType::Map(_) => reader.read_bytes(cursor)?.1,
                DataType::Row(_) => {
                    return Err(IllegalArgument {
                        message: format!(
                            "field {pos} is a nested row, which cannot be read in place"
                        ),
                    });
                }
            };
        }
        Ok(Self {
            row_type,
            reader,
            positions,
        })
    }

    fn position(&self, pos: usize) -> Result<usize> {
        match self.positions.get(pos) {
            Some(&NULL) => Err(IllegalArgument {
                message: format!("field {pos} is null"),
            }),
            Some(&position) => Ok(position),
            None => Err(IllegalArgument {
                message: format!(
                    "position {pos} out of bounds (row has {} fields)",
                    self.positions.len()
                ),
            }),
        }
    }
}

impl InternalRow for IndexedCompactedRow<'_> {
    fn get_field_count(&self) -> usize {
        self.positions.len()
    }

    fn as_encoded_bytes(&self, _write_format: WriteFormat) -> Option<&[u8]> {
        None
    }
}

impl DataGetters for IndexedCompactedRow<'_> {
    fn is_null_at(&self, pos: usize) -> Result<bool> {
        match self.positions.get(pos) {
            Some(&position) => Ok(position == NULL),
            None => Err(IllegalArgument {
                message: format!(
                    "position {pos} out of bounds (row has {} fields)",
                    self.positions.len()
                ),
            }),
        }
    }

    fn get_boolean(&self, pos: usize) -> Result<bool> {
        Ok(self.reader.read_boolean(self.position(pos)?)?.0)
    }

    fn get_byte(&self, pos: usize) -> Result<i8> {
        Ok(self.reader.read_byte(self.position(pos)?)?.0 as i8)
    }

    fn get_short(&self, pos: usize) -> Result<i16> {
        Ok(self.reader.read_short(self.position(pos)?)?.0)
    }

    fn get_int(&self, pos: usize) -> Result<i32> {
        Ok(self.reader.read_int(self.position(pos)?)?.0)
    }

    fn get_long(&self, pos: usize) -> Result<i64> {
        Ok(self.reader.read_long(self.position(pos)?)?.0)
    }

    fn get_float(&self, pos: usize) -> Result<f32> {
        Ok(self.reader.read_float(self.position(pos)?)?.0)
    }

    fn get_double(&self, pos: usize) -> Result<f64> {
        Ok(self.reader.read_double(self.position(pos)?)?.0)
    }

    fn get_char(&self, pos: usize, _length: usize) -> Result<&str> {
        // Like the Java client, the declared length is not checked.
        self.get_string(pos)
    }

    fn get_string(&self, pos: usize) -> Result<&str> {
        Ok(self.reader.read_string(self.position(pos)?)?.0)
    }

    fn get_decimal(&self, pos: usize, precision: usize, scale: usize) -> Result<Decimal> {
        let position = self.position(pos)?;
        let (precision, scale) = (precision as u32, scale as u32);
        let decimal = if Decimal::is_compact_precision(precision) {
            Decimal::from_unscaled_long(self.reader.read_long(position)?.0, precision, scale)
        } else {
            Decimal::from_unscaled_bytes(self.reader.read_bytes(position)?.0, precision, scale)
        };
        decimal.map_err(|e| IllegalArgument {
            message: format!("invalid decimal in field {pos}: {e}"),
        })
    }

    fn get_date(&self, pos: usize) -> Result<Date> {
        Ok(Date::new(self.reader.read_int(self.position(pos)?)?.0))
    }

    fn get_time(&self, pos: usize) -> Result<Time> {
        Ok(Time::new(self.reader.read_int(self.position(pos)?)?.0))
    }

    fn get_timestamp_ntz(&self, pos: usize, precision: u32) -> Result<TimestampNtz> {
        let (millis, next) = self.reader.read_long(self.position(pos)?)?;
        if TimestampNtz::is_compact(precision) {
            return Ok(TimestampNtz::new(millis));
        }
        let nanos = self.reader.read_int(next)?.0;
        TimestampNtz::from_millis_nanos(millis, nanos).map_err(|e| IllegalArgument {
            message: format!("invalid timestamp in field {pos}: {e}"),
        })
    }

    fn get_timestamp_ltz(&self, pos: usize, precision: u32) -> Result<TimestampLtz> {
        let (millis, next) = self.reader.read_long(self.position(pos)?)?;
        if TimestampLtz::is_compact(precision) {
            return Ok(TimestampLtz::new(millis));
        }
        let nanos = self.reader.read_int(next)?.0;
        TimestampLtz::from_millis_nanos(millis, nanos).map_err(|e| IllegalArgument {
            message: format!("invalid timestamp in field {pos}: {e}"),
        })
    }

    fn get_binary(&self, pos: usize, _length: usize) -> Result<&[u8]> {
        self.get_bytes(pos)
    }

    fn get_bytes(&self, pos: usize) -> Result<&[u8]> {
        Ok(self.reader.read_bytes(self.position(pos)?)?.0)
    }

    fn get_array(&self, pos: usize) -> Result<ArrayView<'_>> {
        let bytes = self.reader.read_bytes(self.position(pos)?)?.0;
        Ok(ArrayView::Binary(FlussArray::from_bytes(bytes)?))
    }

    fn get_map(&self, pos: usize) -> Result<MapView<'_>> {
        let bytes = self.reader.read_bytes(self.position(pos)?)?.0;
        match &self.row_type.fields()[pos].data_type {
            DataType::Map(map_type) => Ok(MapView::Binary(FlussMap::from_bytes(
                bytes,
                map_type.key_type(),
                map_type.value_type(),
            )?)),
            other => Err(IllegalArgument {
                message: format!("type mismatch at position {pos}: expected Map, got {other:?}"),
            }),
        }
    }
}

#[cfg(test)]
mod tests;
