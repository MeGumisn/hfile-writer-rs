//! Arrow / Parquet -> HFile.
//!
//! One Arrow row becomes one HBase row: the configured key columns are concatenated into the
//! row key and every other column becomes a cell. Cells are sorted with the HBase comparator
//! before they are written, so the resulting file is always valid for `LoadIncrementalHFiles`.

use super::{Mapping, ValueEncoding, encode_value, value_as_i64};
use crate::error::{HFileError, Result, corrupt, invalid};
use crate::hbase::kv::{Cell, CellType, compare_cells};
use crate::hbase::writer::{HFileWriter, WriterOptions};
use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::Path;

/// Statistics of a completed conversion.
#[derive(Debug, Clone, Default)]
pub struct WriteStats {
    pub rows: usize,
    pub cells: usize,
    pub first_row_key: Option<Vec<u8>>,
    pub last_row_key: Option<Vec<u8>>,
}

/// Read every record batch of a Parquet file.
pub fn read_parquet(path: impl AsRef<Path>) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Resolve the column index of a field, with a clear error when it is missing.
fn column_index(batch: &RecordBatch, name: &str) -> Result<usize> {
    batch
        .schema()
        .index_of(name)
        .map_err(|_| HFileError::InvalidArgument(format!("column '{name}' not found in input")))
}

/// The error for a non-empty input that produced no cells at all.
pub(crate) fn no_cells_error(rows: usize) -> HFileError {
    crate::error::corrupt_error(format!(
        "no cells were produced from {rows} input rows: every value column was null, or the \
         mapping skipped them all (check `row_key`, `family` and the timestamp column)"
    ))
}

/// Convert record batches into sorted HBase cells.
pub fn record_batches_to_cells(batches: &[RecordBatch], mapping: &Mapping) -> Result<Vec<Cell>> {
    let mut cells = Vec::new();
    let mut rows = 0usize;
    for batch in batches {
        rows += batch.num_rows();
        cells.extend(batch_to_cells(batch, mapping)?);
    }
    if cells.is_empty() && rows > 0 {
        return Err(no_cells_error(rows));
    }
    cells.sort_by(compare_cells);
    Ok(cells)
}

/// True when the input uses the raw cell layout produced by `hfile2arrow` without a schema.
///
/// The raw layout carries the row key in its own column, so `row_key: ["__rowkey"]` is the
/// explicit spelling of the same thing and must not fall through to the column layout: there
/// every `__*` column is skipped, which would silently produce no cells at all.
pub fn is_raw_layout(schema: &arrow_schema::Schema, mapping: &Mapping) -> bool {
    if schema.index_of("__rowkey").is_err() || schema.index_of("__value").is_err() {
        return false;
    }
    match mapping.row_key.as_slice() {
        [] => true,
        [only] => only == "__rowkey",
        _ => false,
    }
}

fn raw_batch_to_cells(batch: &RecordBatch, mapping: &Mapping) -> Result<Vec<Cell>> {
    let schema = batch.schema();
    let rowkey_index = column_index(batch, "__rowkey")?;
    let value_index = column_index(batch, "__value")?;
    let family_index = schema.index_of("__family").ok();
    let qualifier_index = schema.index_of("__qualifier").ok();
    let timestamp_index = schema.index_of("__timestamp").ok();
    let type_index = schema.index_of("__cell_type").ok();

    let default_family = mapping.family.clone();
    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let Some(row_key) = encode_value(
            batch.column(rowkey_index).as_ref(),
            row,
            ValueEncoding::Binary,
        )?
        else {
            return corrupt("__rowkey column contains a null value");
        };
        let family = match family_index {
            Some(index) => encode_value(batch.column(index).as_ref(), row, ValueEncoding::Binary)?
                .unwrap_or_default(),
            None => match &default_family {
                Some(family) => family.as_bytes().to_vec(),
                None => return invalid("raw input has no __family column and no default family"),
            },
        };
        let qualifier = match qualifier_index {
            Some(index) => encode_value(batch.column(index).as_ref(), row, ValueEncoding::Binary)?
                .unwrap_or_default(),
            None => Vec::new(),
        };
        let value = match encode_value(
            batch.column(value_index).as_ref(),
            row,
            ValueEncoding::Binary,
        )? {
            Some(value) => value,
            None => Vec::new(),
        };
        let timestamp = match timestamp_index {
            Some(index) => value_as_i64(batch.column(index).as_ref(), row)?
                .unwrap_or(mapping.timestamp_or_latest()),
            None => mapping.timestamp_or_latest(),
        };
        let cell_type = match type_index {
            Some(index) => {
                let raw = encode_value(batch.column(index).as_ref(), row, ValueEncoding::Binary)?
                    .unwrap_or_default();
                CellType::from_code(raw.first().copied().unwrap_or(4))?
            }
            None => CellType::Put,
        };
        out.push(Cell {
            row: row_key,
            family,
            qualifier,
            timestamp,
            cell_type,
            value,
            tags: Vec::new(),
            seq_id: 0,
        });
    }
    Ok(out)
}

/// Convert one record batch into cells (used by [`crate::arrow::async_io`] as well).
pub(crate) fn batch_to_cells(batch: &RecordBatch, mapping: &Mapping) -> Result<Vec<Cell>> {
    let schema = batch.schema();
    if is_raw_layout(&schema, mapping) {
        return raw_batch_to_cells(batch, mapping);
    }
    let separator = mapping.separator_bytes();

    // Where does the row key come from?
    let use_rowkey_column = mapping.row_key.is_empty();
    let row_key_indices: Vec<usize> = if use_rowkey_column {
        if schema.index_of("__rowkey").is_err() {
            return invalid(
                "no row key configured: set `row_key` in the mapping or provide a '__rowkey' column",
            );
        }
        vec![column_index(batch, "__rowkey")?]
    } else {
        mapping
            .row_key
            .iter()
            .map(|name| column_index(batch, name))
            .collect::<Result<Vec<_>>>()?
    };

    let timestamp_index = match &mapping.timestamp_column {
        Some(name) => Some(column_index(batch, name)?),
        None => None,
    };

    // Every non key, non timestamp column becomes a cell.
    let mut value_columns = Vec::new();
    for (i, field) in schema.fields().iter().enumerate() {
        let name = field.name();
        if row_key_indices.contains(&i) || Some(i) == timestamp_index {
            continue;
        }
        if name.starts_with("__") {
            continue;
        }
        let (family, qualifier) = mapping.split_column_name(name)?;
        value_columns.push((i, family, qualifier));
    }

    let mut out = Vec::with_capacity(batch.num_rows() * value_columns.len().max(1));
    for row in 0..batch.num_rows() {
        let row_key = if use_rowkey_column {
            match encode_value(
                batch.column(row_key_indices[0]).as_ref(),
                row,
                ValueEncoding::Binary,
            )? {
                Some(bytes) => bytes,
                None => return corrupt("row key column contains a null value"),
            }
        } else {
            let mut key = Vec::new();
            for (n, index) in row_key_indices.iter().enumerate() {
                if n > 0 {
                    key.extend_from_slice(&separator);
                }
                match encode_value(batch.column(*index).as_ref(), row, ValueEncoding::Binary)? {
                    Some(bytes) => key.extend_from_slice(&bytes),
                    None => return corrupt("row key column contains a null value"),
                }
            }
            key
        };

        let timestamp = match timestamp_index {
            Some(index) => value_as_i64(batch.column(index).as_ref(), row)?
                .unwrap_or(mapping.timestamp_or_latest()),
            None => mapping.timestamp_or_latest(),
        };

        for (index, family, qualifier) in &value_columns {
            let Some(value) =
                encode_value(batch.column(*index).as_ref(), row, mapping.value_encoding)?
            else {
                continue;
            };
            out.push(Cell {
                row: row_key.clone(),
                family: family.clone(),
                qualifier: qualifier.clone(),
                timestamp,
                cell_type: CellType::Put,
                value,
                tags: Vec::new(),
                seq_id: 0,
            });
        }
    }
    Ok(out)
}

/// Write already sorted cells to an HFile.
pub fn write_cells(
    path: impl AsRef<Path>,
    cells: Vec<Cell>,
    options: WriterOptions,
) -> Result<WriteStats> {
    if cells.is_empty() {
        return invalid("cannot write an HFile without any cells");
    }
    let first_row_key = Some(cells.first().expect("not empty").row.clone());
    let last_row_key = Some(cells.last().expect("not empty").row.clone());
    let count = cells.len();
    let mut rows = 1usize;
    for pair in cells.windows(2) {
        if pair[0].row != pair[1].row {
            rows += 1;
        }
    }
    let mut writer = HFileWriter::create(path, options)?;
    writer.append_all(cells)?;
    writer.finish()?;
    Ok(WriteStats {
        rows,
        cells: count,
        first_row_key,
        last_row_key,
    })
}

/// Convert a Parquet file into an HFile.
pub fn parquet_to_hfile(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    mapping: &Mapping,
    options: WriterOptions,
) -> Result<WriteStats> {
    let batches = read_parquet(input)?;
    let cells = record_batches_to_cells(&batches, mapping)?;
    write_cells(output, cells, options)
}

/// Compute the Arrow schema that [`write::parquet_to_hfile`] produced cells for, so the reverse
/// conversion can restore types. Returns the schema of the Parquet input.
pub fn source_schema(input: impl AsRef<Path>) -> Result<arrow_schema::SchemaRef> {
    let file = File::open(input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    Ok(builder.schema().clone())
}

/// Write an Arrow schema to JSON so it can be passed back to `hfile2arrow`.
pub fn save_schema(path: impl AsRef<Path>, schema: &arrow_schema::Schema) -> Result<()> {
    let json = serde_json::to_string_pretty(schema)
        .map_err(|e| HFileError::InvalidArgument(format!("cannot serialize schema: {e}")))?;
    std::fs::write(path, json)?;
    Ok(())
}

pub fn load_schema(path: impl AsRef<Path>) -> Result<arrow_schema::SchemaRef> {
    let text = std::fs::read_to_string(path)?;
    let schema: arrow_schema::Schema = serde_json::from_str(&text)
        .map_err(|e| HFileError::InvalidArgument(format!("invalid schema file: {e}")))?;
    Ok(std::sync::Arc::new(schema))
}
