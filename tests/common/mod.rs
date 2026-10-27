//! Helpers shared by the integration tests.

#![allow(dead_code)]

use hfile_rs::hbase::{Cell, HFileReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

/// A unique directory below the OS temp dir, removed when it goes out of scope.
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub fn new(tag: &str) -> Self {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("hfile-rs-test-{tag}-{}-{id}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create temp dir");
        TempDir { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn join(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// `rows` rows with two qualifiers each, already in HBase comparator order.
pub fn sample_cells(rows: usize) -> Vec<Cell> {
    let mut cells = Vec::with_capacity(rows * 2);
    for row in 0..rows {
        for qualifier in 0..2 {
            let mut cell = Cell::new(
                format!("row{row:05}").into_bytes(),
                b"f".to_vec(),
                format!("q{qualifier}").into_bytes(),
                format!("value-{row:05}-{qualifier}").into_bytes(),
            );
            cell.timestamp = 1_000_000 + row as i64;
            cell.seq_id = (row * 2 + qualifier) as u64 + 1;
            cells.push(cell);
        }
    }
    cells
}

/// One canonical line per cell, used in assertion messages.
pub fn cell_line(cell: &Cell) -> String {
    format!(
        "{}/{}/{} ts={} type={} seq={} value={}",
        String::from_utf8_lossy(&cell.row),
        String::from_utf8_lossy(&cell.family),
        String::from_utf8_lossy(&cell.qualifier),
        cell.timestamp,
        cell.cell_type.code(),
        cell.seq_id,
        String::from_utf8_lossy(&cell.value),
    )
}

pub fn cell_lines(cells: &[Cell]) -> Vec<String> {
    cells.iter().map(cell_line).collect()
}

/// Read every cell, failing the test on any reader error.
pub fn read_all(path: impl AsRef<Path>) -> Vec<Cell> {
    HFileReader::open(path)
        .expect("open hfile")
        .read_all()
        .expect("read cells")
}

/// A tiny wide-layout batch: `id` (row key), `name` and `cf:score`.
pub fn demo_batch() -> arrow_array::RecordBatch {
    use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("cf:score", DataType::Float64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("alice"), None, Some("carol")])),
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None])),
        ],
    )
    .expect("build batch")
}

/// Write a record batch as Parquet so the CLI tests have real input files.
pub fn write_parquet(path: impl AsRef<Path>, batch: &arrow_array::RecordBatch) {
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).expect("parquet writer");
    writer.write(batch).expect("write parquet");
    writer.close().expect("close parquet");
}
