//! Command line interface.
//!
//! ```text
//! hfile info <file.hfile> [--dump] [--limit N]
//! hfile dump <file.hfile>
//! hfile bloom <file.hfile> <row>...          # bloom filter membership, `hex:aabb` for binary rows
//! hfile meta <file.hfile> <name>             # payload of one meta block
//! hfile arrow2hfile <input.parquet> --out <file.hfile> [--family cf] [--bloom ROW] [--meta N=V]
//! hfile hfile2arrow <file.hfile> --out <output.parquet> [--schema schema.json]
//! hfile arrow2bulkload <input.parquet> --dir <bulkdir> --family cf
//! ```

use clap::{Parser, Subcommand};
use hfile_rs::arrow::read::{ReadOptions, hfile_to_parquet, raw_schema};
use hfile_rs::arrow::write::{load_schema, parquet_to_hfile, save_schema, source_schema};
use hfile_rs::arrow::{Mapping, ValueEncoding};
use hfile_rs::error::{HFileError, Result};
use hfile_rs::hbase::block::DEFAULT_BLOCK_SIZE;
use hfile_rs::hbase::bloom::{BloomOptions, BloomType, DEFAULT_ERROR_RATE};
use hfile_rs::hbase::compression::{Compression, ZstdFraming};
use hfile_rs::hbase::encoding::DataBlockEncoding;
use hfile_rs::hbase::reader::HFileReader;
use hfile_rs::hbase::writer::WriterOptions;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "hfile",
    version,
    about = "Read/write Apache HBase HFile v3 files and convert them to/from Arrow"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print the trailer, file info and statistics of an HFile.
    Info {
        /// Path of the HFile.
        path: PathBuf,
        /// Print every cell (up to --limit).
        #[arg(long)]
        dump: bool,
        /// Maximum number of cells to print with --dump.
        #[arg(long, default_value_t = 20)]
        limit: usize,
        /// Skip checksum verification.
        #[arg(long)]
        no_checksum: bool,
    },

    /// Print every cell in a canonical one-line-per-cell format.
    Dump {
        /// Path of the HFile.
        path: PathBuf,
    },

    /// Ask the bloom filter which of the given rows may be in the file.
    Bloom {
        /// Path of the HFile.
        path: PathBuf,
        /// Rows to test; printed as `<row> maybe|no`.
        rows: Vec<String>,
    },

    /// Print a meta block by name.
    Meta {
        /// Path of the HFile.
        path: PathBuf,
        /// Meta block to print.
        name: String,
    },

    /// Convert a Parquet file into an HFile.
    Arrow2hfile(Box<ArrowToHFileArgs>),

    /// Convert an HFile into a Parquet file.
    Hfile2arrow(Box<HFileToArrowArgs>),

    /// Convert a Parquet file and place it in a bulk load directory layout.
    Arrow2bulkload(Box<BulkLoadArgs>),
}

#[derive(Debug, Parser)]
struct ArrowToHFileArgs {
    /// Input Parquet file.
    input: PathBuf,
    /// Output HFile path.
    #[arg(long)]
    out: PathBuf,
    /// JSON mapping file (see docs/arrow-integration.md).
    #[arg(long)]
    mapping: Option<PathBuf>,
    /// Default column family for fields without a "family:" prefix.
    #[arg(long)]
    family: Option<String>,
    /// Columns concatenated into the row key.
    #[arg(long, value_delimiter = ',')]
    row_key: Vec<String>,
    /// Column holding the HBase timestamp (milliseconds).
    #[arg(long)]
    timestamp_column: Option<String>,
    /// Fixed timestamp used when --timestamp-column is absent.
    #[arg(long)]
    timestamp: Option<i64>,
    /// Value encoding: text or binary.
    #[arg(long, default_value = "text")]
    value_encoding: String,
    /// Data block encoding: NONE, PREFIX or ROW_INDEX_V1.
    #[arg(long, default_value = "NONE")]
    encoding: String,
    /// Block compression: none, gz, snappy or zstd.
    #[arg(long, default_value = "none")]
    codec: String,
    /// zstd payload framing: raw (Hadoop `ZStandardCodec`) or hbase (`hbase-compression-zstd`).
    #[arg(long, default_value = "raw")]
    zstd_framing: String,
    /// Bloom filter type: none or ROW.
    #[arg(long, default_value = "none")]
    bloom: String,
    /// Bloom filter false positive rate (HBase `io.storefile.bloom.error.rate`).
    #[arg(long)]
    bloom_error_rate: Option<f32>,
    /// Meta block to add, as `NAME=VALUE`; repeatable.
    #[arg(long, value_name = "NAME=VALUE")]
    meta: Vec<String>,
    /// Target data block size in bytes.
    #[arg(long)]
    block_size: Option<usize>,
    /// Write the input Arrow schema to this JSON file (for the reverse conversion).
    #[arg(long)]
    schema_out: Option<PathBuf>,
}

#[derive(Debug, Parser)]
struct HFileToArrowArgs {
    /// Input HFile.
    input: PathBuf,
    /// Output Parquet file.
    #[arg(long)]
    out: PathBuf,
    /// JSON mapping file, must match the mapping used to write the file.
    #[arg(long)]
    mapping: Option<PathBuf>,
    /// Default column family for fields without a "family:" prefix.
    #[arg(long)]
    family: Option<String>,
    /// Columns that were concatenated into the row key.
    #[arg(long, value_delimiter = ',')]
    row_key: Vec<String>,
    /// Column holding the HBase timestamp.
    #[arg(long)]
    timestamp_column: Option<String>,
    /// Value encoding used when the file was written.
    #[arg(long, default_value = "text")]
    value_encoding: String,
    /// Arrow schema JSON (from `arrow2hfile --schema-out`). Without it the raw cell layout is used.
    #[arg(long)]
    schema: Option<PathBuf>,
    /// Rows per record batch.
    #[arg(long, default_value_t = 8192)]
    batch_size: usize,
    /// Maximum number of rows to convert.
    #[arg(long)]
    limit: Option<usize>,
    /// Print the schema that would be used and exit.
    #[arg(long)]
    print_schema: bool,
}

#[derive(Debug, Parser)]
struct BulkLoadArgs {
    /// Input Parquet file.
    input: PathBuf,
    /// Bulk load output directory (the directory passed to LoadIncrementalHFiles).
    #[arg(long)]
    dir: PathBuf,
    /// Column family (also the sub directory name).
    #[arg(long)]
    family: String,
    /// File name inside the family directory.
    #[arg(long)]
    file_name: Option<String>,
    #[arg(long)]
    mapping: Option<PathBuf>,
    /// Columns concatenated into the row key.
    #[arg(long, value_delimiter = ',')]
    row_key: Vec<String>,
    /// Column holding the HBase timestamp (milliseconds).
    #[arg(long)]
    timestamp_column: Option<String>,
    /// Fixed timestamp used when --timestamp-column is absent.
    #[arg(long)]
    timestamp: Option<i64>,
    #[arg(long, default_value = "text")]
    value_encoding: String,
    #[arg(long, default_value = "NONE")]
    encoding: String,
    #[arg(long, default_value = "none")]
    codec: String,
    #[arg(long, default_value = "raw")]
    zstd_framing: String,
    /// Bloom filter type: none or ROW.
    #[arg(long, default_value = "none")]
    bloom: String,
    /// Bloom filter false positive rate (HBase `io.storefile.bloom.error.rate`).
    #[arg(long)]
    bloom_error_rate: Option<f32>,
    /// Meta block to add, as `NAME=VALUE`; repeatable.
    #[arg(long, value_name = "NAME=VALUE")]
    meta: Vec<String>,
    #[arg(long)]
    block_size: Option<usize>,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Info {
            path,
            dump,
            limit,
            no_checksum,
        } => info(&path, dump, limit, no_checksum),
        Command::Dump { path } => dump(&path),
        Command::Bloom { path, rows } => bloom(&path, &rows),
        Command::Meta { path, name } => meta_block(&path, &name),
        Command::Arrow2hfile(args) => arrow2hfile(*args),
        Command::Hfile2arrow(args) => hfile2arrow(*args),
        Command::Arrow2bulkload(args) => arrow2bulkload(*args),
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Canonical cell dump, used to diff this implementation against real HBase.
fn dump(path: &std::path::Path) -> Result<()> {
    let reader = HFileReader::open(path)?;
    let mut count = 0u64;
    for cell in reader.iter() {
        let cell = cell?;
        count += 1;
        println!(
            "{}/{}/{} ts={} type={} seq={} value={}",
            hex(&cell.row),
            hex(&cell.family),
            hex(&cell.qualifier),
            cell.timestamp,
            cell.cell_type.code(),
            cell.seq_id,
            hex(&cell.value)
        );
    }
    println!("READ_OK {count}");
    Ok(())
}

/// `hfile bloom`: consult the bloom filter, printed as `<row> maybe|no`.
fn bloom(path: &std::path::Path, rows: &[String]) -> Result<()> {
    let reader = HFileReader::open(path)?;
    match reader.bloom_filter() {
        Some(filter) => eprintln!(
            "bloom: {} chunks, {} bytes, {} hash functions, {} keys",
            filter.chunks.len(),
            filter.byte_size(),
            filter.hash_count,
            filter.total_key_count
        ),
        None => eprintln!("bloom: none in this file, every row answers 'maybe'"),
    }
    for row in rows {
        let answer = if reader.may_contain_row(&row_bytes(row)?)? {
            "maybe"
        } else {
            "no"
        };
        println!("{row} {answer}");
    }
    Ok(())
}

/// Row keys are arbitrary bytes; `hex:aabb` spells the ones that are not printable.
fn row_bytes(spec: &str) -> Result<Vec<u8>> {
    let Some(digits) = spec.strip_prefix("hex:") else {
        return Ok(spec.as_bytes().to_vec());
    };
    if digits.len() % 2 != 0 {
        return Err(HFileError::InvalidArgument(format!(
            "row '{spec}' has an odd number of hex digits"
        )));
    }
    digits
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair)
                .map_err(|_| HFileError::InvalidArgument(format!("row '{spec}' is not hex")))?;
            u8::from_str_radix(pair, 16)
                .map_err(|_| HFileError::InvalidArgument(format!("row '{spec}' is not hex")))
        })
        .collect()
}

/// `hfile meta`: print a meta block, hex when it is not valid UTF-8.
fn meta_block(path: &std::path::Path, name: &str) -> Result<()> {
    let reader = HFileReader::open(path)?;
    let names: Vec<String> = reader
        .meta_block_names()
        .map(|n| String::from_utf8_lossy(n).into_owned())
        .collect();
    match reader.meta_block(name.as_bytes())? {
        Some(payload) => {
            if payload
                .iter()
                .all(|b| b.is_ascii_graphic() || matches!(b, b'\n' | b'\r' | b'\t' | b' '))
            {
                println!("{}", String::from_utf8_lossy(&payload));
            } else {
                println!("{}", hex(&payload));
            }
            Ok(())
        }
        None => {
            eprintln!("no meta block '{name}'; the file has: {names:?}");
            std::process::exit(1);
        }
    }
}

fn info(path: &std::path::Path, dump: bool, limit: usize, no_checksum: bool) -> Result<()> {
    let reader = HFileReader::open_with_options(
        path,
        hfile_rs::hbase::reader::ReaderOptions {
            verify_checksums: !no_checksum,
        },
    )?;
    let trailer = reader.trailer();
    println!("file: {}", path.display());
    println!("size: {} bytes", reader.file_size());
    println!(
        "version: {}.{}",
        trailer.major_version, trailer.minor_version
    );
    println!("comparator: {}", trailer.comparator_class_name);
    println!(
        "compression: {} ({})",
        reader.compression().name(),
        trailer.compression_codec
    );
    println!(
        "data block encoding: {}",
        reader.data_block_encoding().name()
    );
    println!("entries: {}", reader.entry_count());
    println!("data blocks: {}", reader.data_block_count());
    println!("index levels: {}", trailer.num_data_index_levels);
    println!("data index count: {}", trailer.data_index_count);
    println!("meta index count: {}", trailer.meta_index_count);
    println!(
        "first data block offset: {}",
        trailer.first_data_block_offset
    );
    println!("last data block offset: {}", trailer.last_data_block_offset);
    println!("file info offset: {}", trailer.file_info_offset);
    println!("load on open offset: {}", trailer.load_on_open_data_offset);
    println!(
        "uncompressed data index size: {}",
        trailer.uncompressed_data_index_size
    );
    println!(
        "total uncompressed bytes: {}",
        trailer.total_uncompressed_bytes
    );
    println!("includes tags: {}", reader.includes_tags());
    println!("includes mvcc: {}", reader.includes_mvcc());
    match reader.bloom_filter() {
        Some(filter) => println!(
            "bloom filter: {} ({} chunks, {} bytes, {} hash functions, {} keys)",
            reader
                .file_info()
                .bloom_filter_type()
                .unwrap_or_else(|| "ROW".into()),
            filter.chunks.len(),
            filter.byte_size(),
            filter.hash_count,
            filter.total_key_count
        ),
        None => println!(
            "bloom filter: none{}",
            reader
                .file_info()
                .bloom_filter_type()
                .map(|t| format!(" (file info says {t})"))
                .unwrap_or_default()
        ),
    }
    let meta_names: Vec<String> = reader
        .meta_block_names()
        .map(|n| String::from_utf8_lossy(n).into_owned())
        .collect();
    println!("meta blocks: {meta_names:?}");
    println!("file info:");
    for (key, value) in reader.file_info().iter() {
        let key = String::from_utf8_lossy(key);
        if value.len() <= 64 && value.iter().all(|b| b.is_ascii_graphic()) {
            println!("  {key} = {:?}", String::from_utf8_lossy(value).to_string());
        } else if value.len() == 4 {
            let mut raw = [0u8; 4];
            raw.copy_from_slice(value);
            println!("  {key} = {} (i32)", i32::from_be_bytes(raw));
        } else if value.len() == 8 {
            let mut raw = [0u8; 8];
            raw.copy_from_slice(value);
            println!("  {key} = {} (i64)", i64::from_be_bytes(raw));
        } else {
            println!("  {key} = {} bytes", value.len());
        }
    }
    println!("first cell: {:?}", reader.first_cell()?);
    println!("last cell: {:?}", reader.last_cell()?);
    if dump {
        let mut printed = 0usize;
        for cell in reader.iter() {
            let cell = cell?;
            if printed >= limit {
                break;
            }
            println!(
                "{} / {}:{} ts={} type={:?} seq={} value={:?}",
                String::from_utf8_lossy(&cell.row),
                String::from_utf8_lossy(&cell.family),
                String::from_utf8_lossy(&cell.qualifier),
                cell.timestamp,
                cell.cell_type,
                cell.seq_id,
                String::from_utf8_lossy(&cell.value)
            );
            printed += 1;
        }
    }
    Ok(())
}

fn build_mapping(
    file: &Option<PathBuf>,
    family: Option<String>,
    row_key: Vec<String>,
    timestamp_column: Option<String>,
    timestamp: Option<i64>,
    value_encoding: &str,
) -> Result<Mapping> {
    let mut mapping = match file {
        Some(path) => Mapping::from_json_file(path)?,
        None => Mapping::default(),
    };
    if family.is_some() {
        mapping.family = family;
    }
    if !row_key.is_empty() {
        mapping.row_key = row_key;
    }
    if timestamp_column.is_some() {
        mapping.timestamp_column = timestamp_column;
    }
    if timestamp.is_some() {
        mapping.timestamp = timestamp;
    }
    if !value_encoding.is_empty() {
        mapping.value_encoding = ValueEncoding::parse(value_encoding)?;
    }
    Ok(mapping)
}

fn build_writer_options(
    encoding: &str,
    codec: &str,
    zstd_framing: &str,
    bloom: &str,
    bloom_error_rate: Option<f32>,
    meta: &[String],
    block_size: Option<usize>,
) -> Result<WriterOptions> {
    let bloom_filter = match BloomType::parse_name(bloom)? {
        BloomType::None => None,
        BloomType::Row => Some(BloomOptions {
            error_rate: bloom_error_rate.unwrap_or(DEFAULT_ERROR_RATE),
            ..Default::default()
        }),
    };
    let mut meta_blocks = Vec::with_capacity(meta.len());
    for spec in meta {
        let (name, value) = spec.split_once('=').ok_or_else(|| {
            HFileError::InvalidArgument(format!("--meta expects NAME=VALUE, got '{spec}'"))
        })?;
        meta_blocks.push((name.to_string(), value.as_bytes().to_vec()));
    }
    Ok(WriterOptions {
        encoding: DataBlockEncoding::parse_name(encoding)?,
        compression: Compression::parse_name(codec)?,
        zstd_framing: ZstdFraming::parse_name(zstd_framing)?,
        bloom_filter,
        meta_blocks,
        block_size: block_size.unwrap_or(DEFAULT_BLOCK_SIZE),
        ..Default::default()
    })
}

fn arrow2hfile(args: ArrowToHFileArgs) -> Result<()> {
    let mapping = build_mapping(
        &args.mapping,
        args.family,
        args.row_key,
        args.timestamp_column,
        args.timestamp,
        &args.value_encoding,
    )?;
    let options = build_writer_options(
        &args.encoding,
        &args.codec,
        &args.zstd_framing,
        &args.bloom,
        args.bloom_error_rate,
        &args.meta,
        args.block_size,
    )?;
    let stats = parquet_to_hfile(&args.input, &args.out, &mapping, options)?;
    if let Some(schema_out) = &args.schema_out {
        let schema = source_schema(&args.input)?;
        save_schema(schema_out, &schema)?;
        println!("schema written to {}", schema_out.display());
    }
    println!(
        "wrote {} cells / {} rows to {} (encoding {}), first row {:?}, last row {:?}",
        stats.cells,
        stats.rows,
        args.out.display(),
        DataBlockEncoding::parse_name(&args.encoding)?.name(),
        stats
            .first_row_key
            .map(|k| String::from_utf8_lossy(&k).to_string()),
        stats
            .last_row_key
            .map(|k| String::from_utf8_lossy(&k).to_string())
    );
    Ok(())
}

fn hfile2arrow(args: HFileToArrowArgs) -> Result<()> {
    let mapping = build_mapping(
        &args.mapping,
        args.family,
        args.row_key,
        args.timestamp_column,
        None,
        &args.value_encoding,
    )?;
    let schema = match &args.schema {
        Some(path) => Some(load_schema(path)?),
        None => None,
    };
    if args.print_schema {
        let schema = schema.clone().unwrap_or_else(raw_schema);
        let json = serde_json::to_string_pretty(schema.as_ref())
            .map_err(|e| HFileError::Arrow(e.to_string()))?;
        println!("{json}");
        return Ok(());
    }
    let options = ReadOptions {
        mapping,
        schema,
        batch_size: args.batch_size,
        limit: args.limit,
    };
    let stats = hfile_to_parquet(&args.input, &args.out, &options)?;
    println!(
        "wrote {} rows ({} cells) to {}",
        stats.rows,
        stats.cells,
        args.out.display()
    );
    Ok(())
}

fn arrow2bulkload(args: BulkLoadArgs) -> Result<()> {
    let mapping = build_mapping(
        &args.mapping,
        None,
        args.row_key,
        args.timestamp_column,
        args.timestamp,
        &args.value_encoding,
    )?;
    let mut mapping = mapping;
    // Fields without a "family:" prefix belong to the bulk load family.
    if mapping.family.is_none() {
        mapping.family = Some(args.family.clone());
    }
    let options = build_writer_options(
        &args.encoding,
        &args.codec,
        &args.zstd_framing,
        &args.bloom,
        args.bloom_error_rate,
        &args.meta,
        args.block_size,
    )?;

    let family_dir = args.dir.join(&args.family);
    std::fs::create_dir_all(&family_dir)?;
    let file_name = args
        .file_name
        .unwrap_or_else(|| format!("{}.hfile", args.family));
    let target = family_dir.join(file_name);
    let stats = parquet_to_hfile(&args.input, &target, &mapping, options)?;

    // _SUCCESS marks the directory as complete for the Hadoop FileOutputCommitter layout.
    std::fs::write(args.dir.join("_SUCCESS"), b"")?;
    println!(
        "wrote {} cells / {} rows to {}",
        stats.cells,
        stats.rows,
        target.display()
    );
    println!(
        "next: hbase org.apache.hadoop.hbase.tool.LoadIncrementalHFiles {} <table>",
        args.dir.display()
    );
    Ok(())
}
