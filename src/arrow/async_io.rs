//! Async Parquet <-> HFile conversion.
//!
//! [`super::read`] and [`super::write`] use the blocking Parquet API. This module uses the
//! official async one (`ParquetRecordBatchStreamBuilder` for reading, `AsyncArrowWriter` for
//! writing) together with [`AsyncHFileReader`] and [`AsyncHFileWriter`], so a conversion never
//! blocks a runtime thread and memory stays bounded by one block (HFile side) or one row group
//! (Parquet side).
//!
//! The mappings, layouts and statistics are exactly those of the blocking functions, so the two
//! directions are interchangeable: a file produced here reads back with `hfile2arrow`, and a
//! Parquet file written by `AsyncArrowWriter` reads back with `arrow::write::read_parquet`.
//!
//! ```no_run
//! use hfile_rs::arrow::Mapping;
//! use hfile_rs::arrow::async_io::{hfile_to_parquet, parquet_to_hfile};
//! use hfile_rs::arrow::read::ReadOptions;
//! use hfile_rs::hbase::WriterOptions;
//!
//! # async fn example() -> hfile_rs::Result<()> {
//! let mapping = Mapping {
//!     row_key: vec!["id".to_string()],
//!     ..Default::default()
//! };
//! parquet_to_hfile("in.parquet", "out.hfile", &mapping, WriterOptions::default()).await?;
//! hfile_to_parquet("out.hfile", "back.parquet", &ReadOptions::default()).await?;
//! # Ok(())
//! # }
//! ```

use super::Mapping;
use super::read::{BatchAssembler, ReadOptions, ReadStats, raw_schema};
use super::write::{WriteStats, batch_to_cells, no_cells_error};
use crate::error::{HFileError, Result, invalid};
use crate::hbase::async_io::{AsyncHFileReader, AsyncHFileWriter};
use crate::hbase::kv::{Cell, compare_cells};
use crate::hbase::writer::WriterOptions;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::StreamExt;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use std::path::Path;

/// Read every record batch of a Parquet file.
pub async fn read_parquet(input: impl AsRef<Path>) -> Result<Vec<RecordBatch>> {
    let file = tokio::fs::File::open(input).await?;
    let builder = ParquetRecordBatchStreamBuilder::new(file).await?;
    let mut stream = builder.build()?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Compute the Arrow schema of a Parquet file without reading its data.
pub async fn source_schema(input: impl AsRef<Path>) -> Result<SchemaRef> {
    let file = tokio::fs::File::open(input).await?;
    let builder = ParquetRecordBatchStreamBuilder::new(file).await?;
    Ok(builder.schema().clone())
}

/// Write already sorted cells to an HFile.
pub async fn write_cells(
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
    let mut writer = AsyncHFileWriter::create(path, options).await?;
    writer.append_all(cells).await?;
    writer.finish().await?;
    Ok(WriteStats {
        rows,
        cells: count,
        first_row_key,
        last_row_key,
    })
}

/// Convert a Parquet file into an HFile.
///
/// Batches are streamed out of Parquet and converted on the blocking pool; only the cells are
/// accumulated, because the writer needs them in HBase comparator order.
pub async fn parquet_to_hfile(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    mapping: &Mapping,
    options: WriterOptions,
) -> Result<WriteStats> {
    let file = tokio::fs::File::open(input).await?;
    let builder = ParquetRecordBatchStreamBuilder::new(file).await?;
    let mut stream = builder.build()?;

    let mut cells = Vec::new();
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        rows += batch.num_rows();
        let mapping = mapping.clone();
        let converted = tokio::task::spawn_blocking(move || batch_to_cells(&batch, &mapping)).await;
        cells.extend(converted.map_err(join_error)??);
    }
    if cells.is_empty() && rows > 0 {
        return Err(no_cells_error(rows));
    }
    let cells = tokio::task::spawn_blocking(move || {
        let mut cells = cells;
        cells.sort_by(compare_cells);
        cells
    })
    .await
    .map_err(join_error)?;

    write_cells(output, cells, options).await
}

/// Convert an HFile into a Parquet file.
///
/// Cells are read one block at a time and handed to `AsyncArrowWriter` as record batches, so the
/// memory high water mark is one HFile block plus one Parquet row group.
pub async fn hfile_to_parquet(
    hfile: impl AsRef<Path>,
    output: impl AsRef<Path>,
    options: &ReadOptions,
) -> Result<ReadStats> {
    let reader = AsyncHFileReader::open(hfile).await?;
    let schema = options.schema.clone().unwrap_or_else(raw_schema);
    let file = tokio::fs::File::create(output).await?;
    let mut writer = AsyncArrowWriter::try_new(file, schema, None)?;

    let mut assembler = BatchAssembler::new(options)?;
    let mut stats = ReadStats::default();
    reader
        .for_each_block(async |_block, cells| {
            for cell in cells {
                if let Some(limit) = options.limit
                    && assembler.rows() >= limit
                {
                    return Ok(false);
                }
                stats.cells += 1;
                if let Some(batch) = assembler.push(cell)? {
                    writer.write(&batch).await?;
                }
            }
            Ok(true)
        })
        .await?;
    if let Some(batch) = assembler.finish()? {
        writer.write(&batch).await?;
    }
    stats.rows = assembler.rows();
    writer.close().await?;
    Ok(stats)
}

fn join_error(error: tokio::task::JoinError) -> HFileError {
    HFileError::InvalidArgument(format!("blocking conversion task failed: {error}"))
}
