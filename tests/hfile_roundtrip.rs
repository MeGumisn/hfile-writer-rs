//! HFile write -> read round trips, block layout and reader behaviour.

mod common;

use common::{TempDir, cell_lines, sample_cells};
use hfile_rs::hbase::file_info::keys;
use hfile_rs::hbase::{
    Cell, CellType, Compression, DataBlockEncoding, HFileReader, HFileWriter, WriterOptions,
    ZstdFraming,
};
use std::path::Path;

const ENCODINGS: [DataBlockEncoding; 3] = [
    DataBlockEncoding::None,
    DataBlockEncoding::Prefix,
    DataBlockEncoding::RowIndexV1,
];

/// Every codec/framing pair the writer supports.
const CODECS: [(Compression, ZstdFraming); 4] = [
    (Compression::None, ZstdFraming::Raw),
    (Compression::Gz, ZstdFraming::Raw),
    (Compression::Zstd, ZstdFraming::Raw),
    (Compression::Zstd, ZstdFraming::HBaseBlock),
];

fn write(path: &Path, cells: &[Cell], options: WriterOptions) {
    let mut writer = HFileWriter::create(path, options).expect("create writer");
    writer
        .append_all(cells.iter().cloned())
        .expect("append cells");
    writer.finish().expect("finish file");
}

#[test]
fn roundtrips_every_encoding_and_codec() {
    let dir = TempDir::new("matrix");
    let cells = sample_cells(40);
    let mut index = 0usize;

    for encoding in ENCODINGS {
        for (codec, framing) in CODECS {
            for includes_mvcc in [true, false] {
                let label = format!(
                    "{} / {} / {} / mvcc={includes_mvcc}",
                    encoding.name(),
                    codec.name(),
                    framing.name()
                );
                let path = dir.join(&format!("{index}.hfile"));
                index += 1;

                write(
                    &path,
                    &cells,
                    WriterOptions {
                        encoding,
                        compression: codec,
                        zstd_framing: framing,
                        block_size: 1024,
                        includes_mvcc,
                        ..Default::default()
                    },
                );

                let reader = HFileReader::open(&path).expect("open written file");
                assert_eq!(reader.data_block_encoding(), encoding, "{label}");
                assert_eq!(reader.compression(), codec, "{label}");
                assert_eq!(reader.entry_count(), cells.len() as u64, "{label}");

                let mut expected: Vec<Cell> = cells.clone();
                if !includes_mvcc {
                    // Without mvcc the sequence id is not stored, so it reads back as 0.
                    for cell in &mut expected {
                        cell.seq_id = 0;
                    }
                }
                assert_eq!(
                    cell_lines(&reader.read_all().expect("read cells")),
                    cell_lines(&expected),
                    "{label}"
                );

                let last = reader.last_cell().expect("last cell").expect("some");
                assert_eq!(
                    last.key_bytes(),
                    cells.last().expect("cells").key_bytes(),
                    "{label}"
                );
            }
        }
    }
}

#[test]
fn file_info_records_encoding_and_statistics() {
    let dir = TempDir::new("info");
    let cells = sample_cells(20);
    for encoding in ENCODINGS {
        let path = dir.join(&format!("{}.hfile", encoding.name()));
        write(
            &path,
            &cells,
            WriterOptions {
                encoding,
                ..Default::default()
            },
        );

        let reader = HFileReader::open(&path).expect("open");
        let recorded = reader.file_info().get(keys::DATA_BLOCK_ENCODING);
        if encoding == DataBlockEncoding::None {
            // HBase omits the key for the default encoding.
            assert!(recorded.is_none(), "NONE must not be recorded");
        } else {
            assert_eq!(
                recorded.map(|v| String::from_utf8_lossy(v).to_string()),
                Some(encoding.name().to_string())
            );
        }
        assert_eq!(reader.file_info().max_seq_id(), cells.len() as u64);
        assert!(reader.file_info().get(keys::LASTKEY).is_some());
    }
}

#[test]
fn multi_block_file_stays_ordered_and_seekable() {
    let dir = TempDir::new("blocks");
    let cells = sample_cells(200);
    let path = dir.join("multi.hfile");
    write(
        &path,
        &cells,
        WriterOptions {
            block_size: 512,
            ..Default::default()
        },
    );

    let reader = HFileReader::open(&path).expect("open");
    assert!(
        reader.data_block_count() > 1,
        "expected several data blocks, got {}",
        reader.data_block_count()
    );
    assert_eq!(
        cell_lines(&reader.read_all().expect("scan")),
        cell_lines(&cells)
    );
    assert_eq!(
        reader.first_cell().expect("first").expect("some").row,
        cells.first().expect("cells").row
    );

    // get() resolves through the block index instead of scanning the file.
    let target = &cells[cells.len() / 2];
    let found = reader
        .get(&target.row, &target.family, &target.qualifier)
        .expect("lookup")
        .expect("cell found through the index");
    assert_eq!(found.timestamp, target.timestamp);
    assert_eq!(found.value, target.value);
}

#[test]
fn seek_returns_the_first_cell_at_or_after_the_key() {
    let dir = TempDir::new("seek");
    let cells = sample_cells(30);
    let path = dir.join("seek.hfile");
    write(&path, &cells, WriterOptions::default());

    let reader = HFileReader::open(&path).expect("open");
    let wanted = cells
        .iter()
        .find(|cell| cell.row == b"row00007" && cell.qualifier == b"q1")
        .expect("target cell");
    let found = reader
        .seek(&wanted.key_bytes())
        .expect("seek")
        .expect("hit");
    assert_eq!(found.key_bytes(), wanted.key_bytes());

    // Anything beyond the last key is not in the file.
    let mut beyond = Cell::new(b"zzzz".to_vec(), b"f".to_vec(), b"q".to_vec(), Vec::new());
    beyond.timestamp = i64::MAX;
    beyond.cell_type = CellType::Maximum;
    assert!(reader.seek(&beyond.key_bytes()).expect("seek").is_none());
}

#[test]
fn corrupted_data_block_is_detected_by_the_checksum() {
    let dir = TempDir::new("corrupt");
    let path = dir.join("clean.hfile");
    write(&path, &sample_cells(5), WriterOptions::default());

    let mut bytes = std::fs::read(&path).expect("read file");
    // 33 bytes of block header, so this lands in the first data block payload.
    bytes[45] ^= 0xff;
    let broken = dir.join("broken.hfile");
    std::fs::write(&broken, &bytes).expect("write broken file");

    let error = HFileReader::open(&broken)
        .expect("open succeeds: only the data section is damaged")
        .read_all()
        .expect_err("checksum verification must fail")
        .to_string();
    assert!(error.contains("checksum"), "unexpected error: {error}");
}

#[test]
fn writing_diff_encodings_is_rejected() {
    let dir = TempDir::new("unsupported");
    for encoding in [DataBlockEncoding::Diff, DataBlockEncoding::FastDiff] {
        let result = HFileWriter::create(
            dir.join("rejected.hfile"),
            WriterOptions {
                encoding,
                ..Default::default()
            },
        );
        let Err(error) = result else {
            panic!("writing {} data blocks must be refused", encoding.name());
        };
        let error = error.to_string();
        assert!(
            error.contains(encoding.name()) && error.contains("not supported"),
            "unexpected error for {}: {error}",
            encoding.name()
        );
    }
}

#[test]
fn reads_the_checked_in_sample_file() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/data/legacy_writer_output.hfile"
    );
    let reader = HFileReader::open(path).expect("open sample");
    assert_eq!(reader.data_block_encoding(), DataBlockEncoding::None);
    assert_eq!(reader.entry_count(), 10);

    let cells = reader.read_all().expect("scan sample");
    assert_eq!(cells.len(), 10);
    assert_eq!(cells[0].row, b"test_row_00");
    assert_eq!(cells[0].family, b"test_cf_00");
    assert_eq!(cells[0].value, b"test_value_00");
    assert_eq!(cells[9].row, b"test_row_09");
}
