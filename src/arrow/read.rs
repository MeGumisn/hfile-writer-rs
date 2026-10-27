//! HFile -> Arrow / Parquet.
//!
//! Two output shapes are supported:
//!
//! * **raw** (no schema): one output row per cell with `__rowkey`, `__family`, `__qualifier`,
//!   `__value`, `__timestamp` and `__cell_type` columns. Lossless and schema free.
//! * **wide** (schema given): one output row per HBase row, columns decoded back into their
//!   Arrow types using the mapping that was used for writing.

use super::{Mapping, ValueEncoding, decode_values};
use crate::error::{HFileError, Result, corrupt, invalid};
use crate::hbase::reader::HFileReader;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

/// Options for the HFile -> Arrow direction.
#[derive(Debug, Clone)]
pub struct ReadOptions {
    /// Mapping used when the HFile was written (needed for the wide mode).
    pub mapping: Mapping,
    /// Arrow schema. When `None` the raw cell layout is produced.
    pub schema: Option<SchemaRef>,
    /// Number of output rows per record batch.
    pub batch_size: usize,
    /// Optional row limit.
    pub limit: Option<usize>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        ReadOptions {
            mapping: Mapping::default(),
            schema: None,
            batch_size: 8192,
            limit: None,
        }
    }
}

/// Statistics of a completed conversion.
#[derive(Debug, Clone, Default)]
pub struct ReadStats {
    pub rows: usize,
    pub cells: usize,
}

/// Schema used when no Arrow schema is supplied.
pub fn raw_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("__rowkey", DataType::Binary, false),
        Field::new("__family", DataType::Binary, false),
        Field::new("__qualifier", DataType::Binary, false),
        Field::new("__value", DataType::Binary, true),
        Field::new("__timestamp", DataType::Int64, false),
        Field::new("__cell_type", DataType::UInt8, false),
    ]))
}

#[derive(Debug, Clone)]
enum ColumnSource {
    /// Part `index` of the row key.
    RowKeyPart(usize),
    /// Cell column identified by family/qualifier.
    Cell(Vec<u8>, Vec<u8>),
    /// Timestamp column: the newest timestamp of the row.
    Timestamp,
}

fn resolve_columns(schema: &SchemaRef, mapping: &Mapping) -> Result<Vec<ColumnSource>> {
    if mapping.row_key.is_empty() {
        return invalid(
            "wide output needs `row_key` in the mapping (use raw mode, without a schema, otherwise)",
        );
    }
    let mut sources = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let name = field.name();
        if let Some(pos) = mapping.row_key.iter().position(|k| k == name) {
            sources.push(ColumnSource::RowKeyPart(pos));
            continue;
        }
        if mapping.timestamp_column.as_deref() == Some(name.as_str()) {
            sources.push(ColumnSource::Timestamp);
            continue;
        }
        let (family, qualifier) = mapping.split_column_name(name)?;
        sources.push(ColumnSource::Cell(family, qualifier));
    }
    Ok(sources)
}

fn split_row(row: &[u8], separator: &[u8], parts: usize) -> Result<Vec<Vec<u8>>> {
    if separator.is_empty() {
        return corrupt("row key separator must not be empty when several key columns are used");
    }
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(parts);
    let mut start = 0usize;
    let mut i = 0usize;
    while i + separator.len() <= row.len() {
        if &row[i..i + separator.len()] == separator {
            out.push(row[start..i].to_vec());
            start = i + separator.len();
            i = start;
        } else {
            i += 1;
        }
    }
    out.push(row[start..].to_vec());
    if out.len() != parts {
        return corrupt(format!(
            "row key splits into {} parts but the mapping declares {parts} key columns",
            out.len()
        ));
    }
    Ok(out)
}

struct BatchBuilder {
    schema: SchemaRef,
    columns: Vec<Vec<Option<Vec<u8>>>>,
    rows: usize,
    batch_size: usize,
    /// One entry per output column: row key and timestamp columns are always rebuilt from raw
    /// big-endian bytes, only cell values follow the mapping's `value_encoding`.
    encodings: Vec<ValueEncoding>,
}

impl BatchBuilder {
    fn new(schema: SchemaRef, batch_size: usize, encodings: Vec<ValueEncoding>) -> Self {
        debug_assert_eq!(encodings.len(), schema.fields().len());
        let columns = vec![Vec::with_capacity(batch_size); schema.fields().len()];
        BatchBuilder {
            schema,
            columns,
            rows: 0,
            batch_size: batch_size.max(1),
            encodings,
        }
    }

    fn uniform(schema: SchemaRef, batch_size: usize, encoding: ValueEncoding) -> Self {
        let encodings = vec![encoding; schema.fields().len()];
        Self::new(schema, batch_size, encodings)
    }

    fn push(&mut self, values: Vec<Option<Vec<u8>>>) -> Result<Option<RecordBatch>> {
        debug_assert_eq!(values.len(), self.columns.len());
        for (i, value) in values.into_iter().enumerate() {
            self.columns[i].push(value);
        }
        self.rows += 1;
        if self.rows >= self.batch_size {
            return self.take_batch().map(Some);
        }
        Ok(None)
    }

    fn take_batch(&mut self) -> Result<RecordBatch> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());
        for (i, field) in self.schema.fields().iter().enumerate() {
            arrays.push(decode_values(
                field.data_type(),
                &self.columns[i],
                self.encodings[i],
            )?);
        }
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| HFileError::Arrow(e.to_string()))?;
        for column in &mut self.columns {
            column.clear();
        }
        self.rows = 0;
        Ok(batch)
    }

    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.rows == 0 {
            return Ok(None);
        }
        Ok(Some(self.take_batch()?))
    }
}

/// Stream HFile contents as record batches.
pub fn for_each_batch<F>(
    reader: &HFileReader,
    options: &ReadOptions,
    mut emit: F,
) -> Result<ReadStats>
where
    F: FnMut(RecordBatch) -> Result<()>,
{
    let mut stats = ReadStats::default();
    match &options.schema {
        None => {
            let schema = raw_schema();
            let mut builder =
                BatchBuilder::uniform(schema, options.batch_size, ValueEncoding::Binary);
            for cell in reader.iter() {
                let cell = cell?;
                if let Some(limit) = options.limit
                    && stats.rows >= limit
                {
                    break;
                }
                stats.cells += 1;
                stats.rows += 1;
                let values = vec![
                    Some(cell.row.clone()),
                    Some(cell.family.clone()),
                    Some(cell.qualifier.clone()),
                    Some(cell.value.clone()),
                    Some(cell.timestamp.to_be_bytes().to_vec()),
                    Some(vec![cell.cell_type.code()]),
                ];
                if let Some(batch) = builder.push(values)? {
                    emit(batch)?;
                }
            }
            if let Some(batch) = builder.finish()? {
                emit(batch)?;
            }
        }
        Some(schema) => {
            let sources = resolve_columns(schema, &options.mapping)?;
            let separator = options.mapping.separator_bytes();
            let encodings = sources
                .iter()
                .map(|source| match source {
                    // Row key parts and timestamps are written as raw big-endian bytes, so they
                    // must be decoded the same way regardless of `value_encoding`.
                    ColumnSource::RowKeyPart(_) | ColumnSource::Timestamp => ValueEncoding::Binary,
                    ColumnSource::Cell(_, _) => options.mapping.value_encoding,
                })
                .collect();
            let mut builder = BatchBuilder::new(schema.clone(), options.batch_size, encodings);

            let mut current_row: Option<Vec<u8>> = None;
            let mut values: Vec<Option<Vec<u8>>> = vec![None; schema.fields().len()];
            let mut row_timestamp = i64::MIN;

            let flush = |row: &[u8],
                         values: &mut Vec<Option<Vec<u8>>>,
                         row_timestamp: i64,
                         builder: &mut BatchBuilder,
                         stats: &mut ReadStats|
             -> Result<Option<RecordBatch>> {
                // Fill row key columns.
                let key_parts = if options.mapping.row_key.len() == 1 {
                    vec![row.to_vec()]
                } else {
                    split_row(row, &separator, options.mapping.row_key.len())?
                };
                for (i, source) in sources.iter().enumerate() {
                    match source {
                        ColumnSource::RowKeyPart(part) => {
                            values[i] = Some(key_parts[*part].clone());
                        }
                        ColumnSource::Timestamp => {
                            values[i] = Some(row_timestamp.to_be_bytes().to_vec());
                        }
                        ColumnSource::Cell(_, _) => {}
                    }
                }
                stats.rows += 1;
                let batch = builder.push(std::mem::take(values))?;
                *values = vec![None; builder.columns.len()];
                Ok(batch)
            };

            for cell in reader.iter() {
                let cell = cell?;
                if let Some(limit) = options.limit
                    && stats.rows >= limit
                {
                    break;
                }
                stats.cells += 1;
                if current_row.as_deref() != Some(cell.row.as_slice()) {
                    let mut batch = None;
                    if let Some(row) = current_row.take() {
                        batch = flush(&row, &mut values, row_timestamp, &mut builder, &mut stats)?;
                    }
                    current_row = Some(cell.row.clone());
                    row_timestamp = i64::MIN;
                    if let Some(batch) = batch {
                        emit(batch)?;
                    }
                }
                row_timestamp = row_timestamp.max(cell.timestamp);
                for (i, source) in sources.iter().enumerate() {
                    if let ColumnSource::Cell(family, qualifier) = source
                        && *family == cell.family
                        && *qualifier == cell.qualifier
                        && values[i].is_none()
                    {
                        // Cells are ordered newest first, so the first one wins.
                        values[i] = Some(cell.value.clone());
                    }
                }
            }
            if let Some(row) = current_row.take() {
                let batch = flush(&row, &mut values, row_timestamp, &mut builder, &mut stats)?;
                if let Some(batch) = batch {
                    emit(batch)?;
                }
            }
            if let Some(batch) = builder.finish()? {
                emit(batch)?;
            }
        }
    }
    Ok(stats)
}

/// Collect all record batches in memory.
pub fn hfile_to_record_batches(
    reader: &HFileReader,
    options: &ReadOptions,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    for_each_batch(reader, options, |batch| {
        batches.push(batch);
        Ok(())
    })?;
    Ok(batches)
}

/// Convert an HFile into a Parquet file.
pub fn hfile_to_parquet(
    hfile: impl AsRef<Path>,
    output: impl AsRef<Path>,
    options: &ReadOptions,
) -> Result<ReadStats> {
    let reader = HFileReader::open(hfile)?;
    let schema = options.schema.clone().unwrap_or_else(raw_schema);
    let file = File::create(output)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    let stats = for_each_batch(&reader, options, |batch| {
        writer.write(&batch)?;
        Ok(())
    })?;
    writer.close()?;
    Ok(stats)
}
