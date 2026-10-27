//! Arrow/Parquet <-> HFile conversions, including both input layouts.

mod common;

use common::{TempDir, cell_lines, read_all, sample_cells};
use hfile_rs::arrow::read::{ReadOptions, hfile_to_parquet, raw_schema};
use hfile_rs::arrow::write::{
    is_raw_layout, load_schema, parquet_to_hfile, read_parquet, record_batches_to_cells,
    save_schema, source_schema,
};
use hfile_rs::arrow::{Mapping, ValueEncoding};
use hfile_rs::hbase::{HFileWriter, WriterOptions, write_hfile};

fn zero_seq(cells: &mut [hfile_rs::hbase::Cell]) {
    for cell in cells {
        cell.seq_id = 0;
    }
}

#[test]
fn wide_layout_roundtrip_through_parquet() {
    let dir = TempDir::new("wide");
    let batch = common::demo_batch();
    let parquet_in = dir.join("in.parquet");
    common::write_parquet(&parquet_in, &batch);

    let hfile = dir.join("wide.hfile");
    let mapping = Mapping {
        row_key: vec!["id".to_string()],
        family: Some("cf".to_string()),
        timestamp: Some(42),
        ..Default::default()
    };
    let stats = parquet_to_hfile(
        &parquet_in,
        &hfile,
        &mapping,
        WriterOptions {
            encoding: hfile_rs::hbase::DataBlockEncoding::RowIndexV1,
            ..Default::default()
        },
    )
    .expect("parquet -> hfile");
    assert_eq!(stats.rows, 3);
    // Row 1 contributes name+score, row 2 only score (name is null), row 3 only name.
    assert_eq!(stats.cells, 4);

    let cells = read_all(&hfile);
    let expected: Vec<(Vec<u8>, String, String)> = vec![
        (
            1i64.to_be_bytes().to_vec(),
            "name".to_string(),
            "alice".to_string(),
        ),
        (
            1i64.to_be_bytes().to_vec(),
            "score".to_string(),
            "1.5".to_string(),
        ),
        (
            2i64.to_be_bytes().to_vec(),
            "score".to_string(),
            "2.5".to_string(),
        ),
        (
            3i64.to_be_bytes().to_vec(),
            "name".to_string(),
            "carol".to_string(),
        ),
    ];
    let actual: Vec<(Vec<u8>, String, String)> = cells
        .iter()
        .map(|cell| {
            (
                cell.row.clone(),
                String::from_utf8_lossy(&cell.qualifier).to_string(),
                String::from_utf8_lossy(&cell.value).to_string(),
            )
        })
        .collect();
    assert_eq!(actual, expected);
    assert!(
        cells
            .iter()
            .all(|cell| cell.family == b"cf" && cell.timestamp == 42)
    );

    // ... and back to Parquet, restoring the Arrow types from the saved schema.
    let parquet_out = dir.join("out.parquet");
    let schema = source_schema(&parquet_in).expect("input schema");
    let read = hfile_to_parquet(
        &hfile,
        &parquet_out,
        &ReadOptions {
            mapping: mapping.clone(),
            schema: Some(schema.clone()),
            batch_size: 2,
            limit: None,
        },
    )
    .expect("hfile -> parquet");
    assert_eq!(read.rows, 3);
    assert_eq!(read.cells, 4);

    let batches = read_parquet(&parquet_out).expect("read back");
    let roundtripped =
        arrow_select::concat::concat_batches(&schema, &batches).expect("concat batches");
    for column in 0..batch.num_columns() {
        assert_eq!(
            format!("{:?}", batch.column(column)),
            format!("{:?}", roundtripped.column(column)),
            "column {} did not survive the round trip",
            batch.schema().field(column).name()
        );
    }
}

#[test]
fn raw_layout_accepts_an_explicit_rowkey_column() {
    let dir = TempDir::new("raw");
    let original = sample_cells(5);
    let hfile = dir.join("raw.hfile");
    write_hfile(&hfile, original.clone(), WriterOptions::default()).expect("write hfile");

    let parquet = dir.join("raw.parquet");
    hfile_to_parquet(&hfile, &parquet, &ReadOptions::default()).expect("hfile -> raw parquet");
    let batches = read_parquet(&parquet).expect("read raw parquet");

    // `row_key: ["__rowkey"]` is the explicit spelling of the raw layout; it must not fall
    // back to the column layout, which used to drop every cell and fail with "no cells".
    let implicit =
        record_batches_to_cells(&batches, &Mapping::default()).expect("implicit raw layout");
    let explicit = record_batches_to_cells(
        &batches,
        &Mapping {
            row_key: vec!["__rowkey".to_string()],
            ..Default::default()
        },
    )
    .expect("explicit raw layout");

    assert_eq!(cell_lines(&explicit), cell_lines(&implicit));
    assert_eq!(explicit.len(), original.len());

    let mut expected = original;
    zero_seq(&mut expected);
    assert_eq!(cell_lines(&explicit), cell_lines(&expected));
}

#[test]
fn is_raw_layout_distinguishes_the_two_input_shapes() {
    let schema = raw_schema();
    assert!(is_raw_layout(&schema, &Mapping::default()));
    assert!(is_raw_layout(
        &schema,
        &Mapping {
            row_key: vec!["__rowkey".to_string()],
            ..Default::default()
        }
    ));
    assert!(!is_raw_layout(
        &schema,
        &Mapping {
            row_key: vec!["id".to_string()],
            ..Default::default()
        }
    ));
    assert!(!is_raw_layout(
        &arrow_schema::Schema::empty(),
        &Mapping::default()
    ));
}

#[test]
fn binary_value_encoding_is_lossless() {
    use arrow_array::{BooleanArray, Int32Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let dir = TempDir::new("binary");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("cf:flag", DataType::Boolean, false),
        Field::new("cf:count", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![7, 8])),
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(Int32Array::from(vec![-1, 1000])),
        ],
    )
    .expect("build batch");

    let parquet_in = dir.join("in.parquet");
    common::write_parquet(&parquet_in, &batch);
    let hfile = dir.join("binary.hfile");
    let mapping = Mapping {
        row_key: vec!["id".to_string()],
        family: Some("cf".to_string()),
        value_encoding: ValueEncoding::Binary,
        timestamp: Some(1),
        ..Default::default()
    };
    parquet_to_hfile(&parquet_in, &hfile, &mapping, WriterOptions::default()).expect("to hfile");

    let cells = read_all(&hfile);
    assert_eq!(cells.len(), 4);
    let flag = cells
        .iter()
        .find(|cell| cell.qualifier == b"flag" && cell.row == 7i64.to_be_bytes())
        .expect("flag cell");
    assert_eq!(flag.value, vec![1]);
    let count = cells
        .iter()
        .find(|cell| cell.qualifier == b"count" && cell.row == 8i64.to_be_bytes())
        .expect("count cell");
    assert_eq!(count.value, 1000i32.to_be_bytes().to_vec());

    let parquet_out = dir.join("out.parquet");
    hfile_to_parquet(
        &hfile,
        &parquet_out,
        &ReadOptions {
            mapping,
            schema: Some(schema.clone()),
            ..Default::default()
        },
    )
    .expect("back to parquet");
    let batches = read_parquet(&parquet_out).expect("read back");
    let roundtripped =
        arrow_select::concat::concat_batches(&schema, &batches).expect("concat batches");
    for column in 0..batch.num_columns() {
        assert_eq!(
            format!("{:?}", batch.column(column)),
            format!("{:?}", roundtripped.column(column)),
            "column {} did not survive the round trip",
            schema.field(column).name()
        );
    }
}

#[test]
fn timestamp_column_roundtrip() {
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let dir = TempDir::new("timestamp-column");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("cf:value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![111, 222])),
            Arc::new(StringArray::from(vec!["first", "second"])),
        ],
    )
    .expect("build batch");

    let parquet_in = dir.join("in.parquet");
    common::write_parquet(&parquet_in, &batch);
    let hfile = dir.join("ts.hfile");
    let mapping = Mapping {
        row_key: vec!["id".to_string()],
        timestamp_column: Some("ts".to_string()),
        family: Some("cf".to_string()),
        ..Default::default()
    };
    parquet_to_hfile(&parquet_in, &hfile, &mapping, WriterOptions::default()).expect("to hfile");
    let cells = read_all(&hfile);
    assert_eq!(cells[0].timestamp, 111);
    assert_eq!(cells[1].timestamp, 222);

    let parquet_out = dir.join("out.parquet");
    hfile_to_parquet(
        &hfile,
        &parquet_out,
        &ReadOptions {
            mapping,
            schema: Some(schema.clone()),
            ..Default::default()
        },
    )
    .expect("back to parquet");
    let batches = read_parquet(&parquet_out).expect("read back");
    let roundtripped =
        arrow_select::concat::concat_batches(&schema, &batches).expect("concat batches");
    for column in 0..batch.num_columns() {
        assert_eq!(
            format!("{:?}", batch.column(column)),
            format!("{:?}", roundtripped.column(column)),
            "column {} did not survive the round trip",
            schema.field(column).name()
        );
    }
}

#[test]
fn schema_json_roundtrip() {
    let dir = TempDir::new("schema");
    let batch = common::demo_batch();
    let path = dir.join("schema.json");
    save_schema(&path, &batch.schema()).expect("save schema");
    let loaded = load_schema(&path).expect("load schema");
    assert_eq!(loaded.as_ref(), batch.schema().as_ref());
}

#[test]
fn empty_input_is_rejected_with_a_helpful_message() {
    let dir = TempDir::new("empty");
    let hfile = dir.join("empty.hfile");
    let Err(error) = write_hfile(&hfile, Vec::new(), WriterOptions::default()) else {
        panic!("an empty file must be refused");
    };
    let error = error.to_string();
    assert!(error.contains("empty"), "unexpected: {error}");

    // A streaming writer that never saw a cell must not produce an unreadable file either.
    let writer = HFileWriter::create(dir.join("other.hfile"), WriterOptions::default())
        .expect("create writer");
    let Err(error) = writer.finish() else {
        panic!("finishing without cells must fail");
    };
    let error = error.to_string();
    assert!(error.contains("empty"), "unexpected: {error}");
}
