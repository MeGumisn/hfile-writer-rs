//! End to end checks of the `hfile` binary.

mod common;

use common::TempDir;
use hfile_rs::hbase::HFileReader;
use std::path::Path;
use std::process::{Command, Output};

const BIN: &str = env!("CARGO_BIN_EXE_hfile");

fn run(args: &[&str]) -> Output {
    Command::new(BIN).args(args).output().expect("run hfile")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

fn s(path: &Path) -> &str {
    path.to_str().expect("utf-8 path")
}

#[test]
fn arrow_to_hfile_to_arrow_through_the_cli() {
    let dir = TempDir::new("cli-roundtrip");
    let parquet = dir.join("in.parquet");
    common::write_parquet(&parquet, &common::demo_batch());
    let hfile = dir.join("out.hfile");
    let schema = dir.join("schema.json");

    let write = run(&[
        "arrow2hfile",
        s(&parquet),
        "--out",
        s(&hfile),
        "--row-key",
        "id",
        "--family",
        "cf",
        "--timestamp",
        "42",
        "--encoding",
        "ROW_INDEX_V1",
        "--codec",
        "zstd",
        "--zstd-framing",
        "hbase",
        "--schema-out",
        s(&schema),
    ]);
    assert!(
        write.status.success(),
        "arrow2hfile failed: {}",
        stderr(&write)
    );
    assert!(
        stdout(&write).contains("wrote 4 cells / 3 rows"),
        "unexpected output: {}",
        stdout(&write)
    );

    let info = run(&["info", s(&hfile)]);
    assert!(info.status.success(), "info failed: {}", stderr(&info));
    let text = stdout(&info);
    for expected in [
        "entries: 4",
        "data block encoding: ROW_INDEX_V1",
        "compression: zstd",
    ] {
        assert!(text.contains(expected), "missing {expected:?} in:\n{text}");
    }

    let dump = run(&["dump", s(&hfile)]);
    assert!(dump.status.success(), "dump failed: {}", stderr(&dump));
    assert!(stdout(&dump).ends_with("READ_OK 4\n"));

    let parquet_out = dir.join("out.parquet");
    let back = run(&[
        "hfile2arrow",
        s(&hfile),
        "--out",
        s(&parquet_out),
        "--schema",
        s(&schema),
        "--row-key",
        "id",
        "--family",
        "cf",
    ]);
    assert!(
        back.status.success(),
        "hfile2arrow failed: {}",
        stderr(&back)
    );
    assert!(parquet_out.exists());
}

#[test]
fn bulkload_writes_the_layout_expected_by_load_incremental_hfiles() {
    let dir = TempDir::new("cli-bulk");
    let parquet = dir.join("in.parquet");
    common::write_parquet(&parquet, &common::demo_batch());
    let bulk = dir.join("bulk");

    let out = run(&[
        "arrow2bulkload",
        s(&parquet),
        "--dir",
        s(&bulk),
        "--family",
        "cf",
        "--row-key",
        "id",
        "--timestamp",
        "42",
        "--encoding",
        "PREFIX",
        "--codec",
        "gz",
    ]);
    assert!(
        out.status.success(),
        "arrow2bulkload failed: {}",
        stderr(&out)
    );

    assert!(bulk.join("_SUCCESS").is_file(), "_SUCCESS marker missing");
    let family_file = bulk.join("cf").join("cf.hfile");
    assert!(family_file.is_file(), "family file missing");

    let reader = HFileReader::open(&family_file).expect("open bulk loaded file");
    assert_eq!(reader.entry_count(), 4);
    assert_eq!(reader.read_all().expect("scan").len(), 4);
}

#[test]
fn raw_layout_accepts_an_explicit_rowkey_column_on_the_command_line() {
    let dir = TempDir::new("cli-raw");
    let parquet = dir.join("wide.parquet");
    common::write_parquet(&parquet, &common::demo_batch());

    let hfile = dir.join("wide.hfile");
    let wide = run(&[
        "arrow2hfile",
        s(&parquet),
        "--out",
        s(&hfile),
        "--row-key",
        "id",
        "--family",
        "cf",
        "--timestamp",
        "42",
    ]);
    assert!(
        wide.status.success(),
        "arrow2hfile failed: {}",
        stderr(&wide)
    );

    let raw = dir.join("raw.parquet");
    let dump = run(&[
        "hfile2arrow",
        s(&hfile),
        "--out",
        s(&raw),
        "--row-key",
        "id",
        "--family",
        "cf",
    ]);
    assert!(
        dump.status.success(),
        "hfile2arrow failed: {}",
        stderr(&dump)
    );

    // Naming the raw layout's own row key column used to produce an empty file.
    let back = dir.join("back.hfile");
    let out = run(&[
        "arrow2hfile",
        s(&raw),
        "--out",
        s(&back),
        "--row-key",
        "__rowkey",
        "--family",
        "cf",
    ]);
    assert!(
        out.status.success(),
        "raw layout with --row-key __rowkey failed: {}",
        stderr(&out)
    );
    assert_eq!(
        HFileReader::open(&back).expect("open").entry_count(),
        4,
        "cells were dropped"
    );
}

#[test]
fn unsupported_encoding_fails_with_a_clear_message() {
    let dir = TempDir::new("cli-invalid");
    let parquet = dir.join("in.parquet");
    common::write_parquet(&parquet, &common::demo_batch());

    let out = run(&[
        "arrow2hfile",
        s(&parquet),
        "--out",
        s(&dir.join("bad.hfile")),
        "--row-key",
        "id",
        "--family",
        "cf",
        "--encoding",
        "DIFF",
    ]);
    assert!(!out.status.success(), "DIFF must not be writable");
    let message = stderr(&out);
    assert!(
        message.contains("DIFF") && message.contains("not supported"),
        "unexpected error: {message}"
    );
}

#[test]
fn missing_input_file_reports_an_error() {
    let dir = TempDir::new("cli-missing");
    let out = run(&[
        "arrow2hfile",
        s(&dir.join("absent.parquet")),
        "--out",
        s(&dir.join("out.hfile")),
    ]);
    assert!(!out.status.success());
    assert!(!stderr(&out).is_empty());
}
