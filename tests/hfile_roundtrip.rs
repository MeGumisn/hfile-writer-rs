//! HFile write -> read round trips, block layout and reader behaviour.

mod common;

use common::{TempDir, cell_lines, read_all, sample_cells};
use hfile_rs::HFileError;
use hfile_rs::hbase::file_info::keys;
use hfile_rs::hbase::{
    BloomOptions, Cell, CellType, Compression, DataBlockEncoding, HFileReader, HFileWriter,
    WriterOptions, ZstdFraming,
};
use std::path::Path;

const ENCODINGS: [DataBlockEncoding; 3] = [
    DataBlockEncoding::None,
    DataBlockEncoding::Prefix,
    DataBlockEncoding::RowIndexV1,
];

/// Every codec/framing pair the writer supports.
const CODECS: [(Compression, ZstdFraming); 5] = [
    (Compression::None, ZstdFraming::Raw),
    (Compression::Gz, ZstdFraming::Raw),
    (Compression::Snappy, ZstdFraming::Raw),
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
fn an_in_memory_sink_writes_a_readable_file_of_the_same_size() {
    let dir = TempDir::new("sink");
    let cells = sample_cells(37);
    let options = WriterOptions {
        block_size: 1024,
        ..Default::default()
    };

    let mut writer =
        HFileWriter::<Vec<u8>>::with_sink(Vec::new(), options.clone()).expect("sink writer");
    assert!(writer.path().as_os_str().is_empty());
    writer
        .append_all(cells.iter().cloned())
        .expect("append cells");
    let bytes = writer.finish_into_sink().expect("finish into sink");

    let path = dir.join("sink.hfile");
    std::fs::write(&path, &bytes).expect("write sink bytes");
    let reader = HFileReader::open(&path).expect("open sink file");
    assert_eq!(reader.entry_count(), cells.len() as u64);
    assert_eq!(
        cell_lines(&reader.read_all().expect("read cells")),
        cell_lines(&cells)
    );

    // Same layout as the file based writer; only the create timestamp inside the file info
    // differs, and that is a fixed width i64.
    let file_path = dir.join("file.hfile");
    write(&file_path, &cells, options);
    let from_file = std::fs::read(&file_path).expect("read file");
    assert_eq!(from_file.len(), bytes.len());
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
fn bloom_filter_round_trips_and_is_queried() {
    let dir = TempDir::new("bloom");
    let cells = sample_cells(40);
    let path = dir.join("bloom.hfile");
    write(
        &path,
        &cells,
        WriterOptions {
            bloom_filter: Some(BloomOptions::default()),
            // Bloom chunks are ordinary compressed blocks.
            compression: Compression::Gz,
            block_size: 512,
            ..Default::default()
        },
    );

    let reader = HFileReader::open(&path).expect("open");
    let filter = reader.bloom_filter().expect("file carries a bloom filter");
    assert_eq!(filter.hash_count, 7);
    assert_eq!(filter.chunks.len(), 1, "40 rows fit in one folded chunk");
    assert_eq!(filter.total_key_count, 40);
    assert_eq!(
        reader.file_info().bloom_filter_type().as_deref(),
        Some("ROW")
    );

    for row in 0..40 {
        let row = format!("row{row:05}");
        assert!(
            reader.may_contain_row(row.as_bytes()).expect("query"),
            "false negative for {row}"
        );
    }
    // Sorts before the first key of the file.
    assert!(!reader.may_contain_row(b"aaa").expect("query"));
    // Rows after the file: mostly rejected, and never an error.
    let false_positives = (0..500)
        .filter(|i| {
            reader
                .may_contain_row(format!("zzz{i:04}").as_bytes())
                .expect("query")
        })
        .count();
    assert!(
        false_positives < 50,
        "{false_positives}/500 false positives"
    );

    // Every cell is still readable with the bloom chunks in the data section.
    assert_eq!(read_all(&path), cells);
}

#[test]
fn a_file_without_a_bloom_filter_answers_maybe() {
    let dir = TempDir::new("no-bloom");
    let path = dir.join("plain.hfile");
    write(&path, &sample_cells(4), WriterOptions::default());

    let reader = HFileReader::open(&path).expect("open");
    assert!(reader.bloom_filter().is_none());
    assert!(reader.file_info().bloom_filter_type().is_none());
    assert!(reader.may_contain_row(b"anything").expect("query"));
}

#[test]
fn meta_blocks_round_trip() {
    let dir = TempDir::new("meta");
    let cells = sample_cells(5);
    let path = dir.join("meta.hfile");

    let mut writer = HFileWriter::create(
        &path,
        WriterOptions {
            // Deliberately out of order: the writer sorts them by name.
            meta_blocks: vec![
                ("meta-b".to_string(), b"second".to_vec()),
                ("meta-a".to_string(), b"first".to_vec()),
            ],
            compression: Compression::Zstd,
            includes_mvcc: true,
            ..Default::default()
        },
    )
    .expect("create writer");
    writer.append_all(cells.iter().cloned()).expect("append");
    writer
        .append_meta_block("meta-c", b"third".to_vec())
        .expect("append meta block");
    writer.finish().expect("finish");

    let reader = HFileReader::open(&path).expect("open");
    let names: Vec<String> = reader
        .meta_block_names()
        .map(|name| String::from_utf8_lossy(name).into_owned())
        .collect();
    assert_eq!(names, ["meta-a", "meta-b", "meta-c"]);
    assert_eq!(reader.meta_block_count(), 3);
    assert_eq!(reader.trailer().meta_index_count, 3);
    assert_eq!(
        reader.meta_block(b"meta-a").expect("read"),
        Some(b"first".to_vec())
    );
    assert_eq!(
        reader.meta_block(b"meta-c").expect("read"),
        Some(b"third".to_vec())
    );
    assert_eq!(reader.meta_block(b"nope").expect("read"), None);
    assert_eq!(read_all(&path), cells);
}

#[test]
fn empty_and_repeated_meta_block_names() {
    let dir = TempDir::new("meta-names");
    let cells = sample_cells(3);
    let path = dir.join("meta.hfile");

    // An empty name is a caller error, and the file must not be created for it.
    let rejected = HFileWriter::create(
        &path,
        WriterOptions {
            meta_blocks: vec![(String::new(), b"x".to_vec())],
            ..Default::default()
        },
    );
    assert!(
        matches!(rejected, Err(HFileError::InvalidArgument(_))),
        "an empty meta block name must be rejected"
    );
    assert!(!path.exists(), "a rejected option must not create the file");

    let mut writer = HFileWriter::create(&path, WriterOptions::default()).expect("create");
    assert!(matches!(
        writer.append_meta_block("", b"x".to_vec()),
        Err(HFileError::InvalidArgument(_))
    ));
    writer.append_all(cells.iter().cloned()).expect("append");
    writer
        .append_meta_block("dup", b"first".to_vec())
        .expect("first payload");
    writer
        .append_meta_block("dup", b"second".to_vec())
        .expect("overwriting payload");
    writer.finish().expect("finish");

    let reader = HFileReader::open(&path).expect("open");
    assert_eq!(reader.meta_block_count(), 1, "a repeated name is one block");
    assert_eq!(
        reader.meta_block(b"dup").expect("read"),
        Some(b"second".to_vec()),
        "the last payload wins"
    );
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
