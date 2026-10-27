# hfile-writer-rs

> [English](README.md) | [简体中文](README.zh-CN.md)

纯 Rust 实现的 Apache HBase **HFile v3** 读写库，附带 Arrow/Parquet 转换与命令行工具。本 crate
写出的文件可以被 HBase 自身读取，也能直接交给 `LoadIncrementalHFiles` 做 bulk load。

```bash
cargo build --release          # -> target/release/hfile
hfile --help
```

## 能力矩阵

| 能力 | 读 | 写 |
|------|----|----|
| HFile v3 块、CRC32C/CRC32/NULL 校验和、PBUF 尾部 | 支持 | 支持 |
| 文件信息（统计、last key、mvcc/tags 标志、store file 元数据） | 支持 | 支持 |
| 数据块编码 | `NONE`、`PREFIX`、`DIFF`、`FAST_DIFF`、`ROW_INDEX_V1` | `NONE`、`PREFIX`、`ROW_INDEX_V1` |
| 压缩 | `NONE`、`GZ`、`SNAPPY`、`ZSTD`（两种框架） | `NONE`、`GZ`、`SNAPPY`、`ZSTD`（两种框架） |
| 索引 | 单层与多层 | 单层 |
| Bloom filter（`BLMFMET2` + `BLMFBLK2`） | `ROW` | `ROW` |
| Meta block（`appendMetaBlock`、meta 索引） | 支持 | 支持 |
| 顺序扫描、`seek`、`get` | 支持 | – |
| Tokio 异步读写（`AsyncHFileReader`、`AsyncHFileWriter`） | 支持 | 支持 |
| Tokio 异步 Arrow/Parquet 转换（`arrow::async_io`） | 支持 | 支持 |
| Arrow ⇄ Parquet | raw 与宽表两种布局 | raw 与宽表两种布局 |
| Bulk load 目录布局 | – | 支持 |

未实现：`ROWCOL` bloom filter、加密，以及 `LZO`/`LZ4`/`BZIP2` 编码器。

## 命令行

```bash
# 查看 HBase 写出的文件
hfile info  /path/to/store.hfile --dump --limit 5
hfile dump  /path/to/store.hfile
hfile bloom /path/to/store.hfile row1 row2 row3      # 查 bloom filter，二进制行用 hex:aabb
hfile meta  /path/to/store.hfile my-meta-block

# Parquet -> HFile（宽表布局：一行 Arrow 数据对应一行 HBase 数据）
hfile arrow2hfile input.parquet --out store.hfile \
    --row-key id --family cf --timestamp-column ts \
    --encoding ROW_INDEX_V1 --codec zstd --bloom ROW --meta my-meta-block=hello

# HFile -> Parquet（传入上面保存的 schema 以还原 Arrow 类型）
hfile hfile2arrow store.hfile --out out.parquet \
    --schema schema.json --row-key id --family cf

# Bulk load 布局：<dir>/<family>/<file> 加 _SUCCESS 标记
hfile arrow2bulkload input.parquet --dir /tmp/bulk --family cf --row-key id
# 之后执行：hbase org.apache.hadoop.hbase.tool.LoadIncrementalHFiles /tmp/bulk <table>
```

`--encoding` 接受 `NONE`、`PREFIX`、`DIFF`、`FAST_DIFF`、`ROW_INDEX_V1`（写 `DIFF` /
`FAST_DIFF` 会被明确拒绝），`--codec` 接受 `none`、`gz`、`snappy`、`zstd`，`--zstd-framing` 接受 `raw`
（Hadoop `ZStandardCodec`，默认）或 `hbase`（`hbase-compression-zstd`）。为什么必须区分，
见 [docs/hfile-format.zh-CN.md](docs/hfile-format.zh-CN.md#压缩)。`--bloom` 接受 `none`（默认）
或 `ROW`，`--bloom-error-rate` 覆盖默认的 1%，`--meta NAME=VALUE` 添加 meta block（可重复）。

## 库用法

```rust
use hfile_rs::hbase::{Cell, DataBlockEncoding, HFileReader, HFileWriter, WriterOptions};

fn main() -> hfile_rs::Result<()> {
    let mut writer = HFileWriter::create(
        "/tmp/example.hfile",
        WriterOptions {
            encoding: DataBlockEncoding::RowIndexV1,
            ..Default::default()
        },
    )?;
    writer.append(Cell::new(
        b"row1".to_vec(),
        b"cf".to_vec(),
        b"q".to_vec(),
        b"value".to_vec(),
    ))?;
    writer.finish()?;

    let reader = HFileReader::open("/tmp/example.hfile")?;
    assert_eq!(reader.read_all()?.len(), 1);
    std::fs::remove_file("/tmp/example.hfile").ok();
    Ok(())
}
```

### 异步 I/O

`AsyncHFileReader` / `AsyncHFileWriter` 是上面两个类型的 tokio 版本。它们读写同样的字节，因此
异步代码与同步代码可以互相读取对方写出的文件：

```rust
use hfile_rs::hbase::{AsyncHFileReader, AsyncHFileWriter, Cell, WriterOptions};

async fn example() -> hfile_rs::Result<()> {
    let mut writer = AsyncHFileWriter::create("/tmp/async.hfile", WriterOptions::default()).await?;
    writer
        .append(Cell::new(b"row1".to_vec(), b"cf".to_vec(), b"q".to_vec(), b"v".to_vec()))
        .await?;
    writer.finish().await?;

    let reader = AsyncHFileReader::open("/tmp/async.hfile").await?;
    reader
        .for_each(|cell| {
            assert_eq!(cell.row, b"row1");
            Ok(())
        })
        .await?;
    Ok(())
}
```

写入器核心（`HFileWriter<W: Write>`）本身不做 I/O，除非 sink 做：`create` 内部套一个
`BufWriter<File>`，`with_sink` 接受任意 `Write`，`take_buffered` 在用 `Vec<u8>` 作 sink 时交出
一个已完成块的全部字节。`AsyncHFileWriter` 基于它实现，每次 `write_all` 只写一个块，因此无论
文件多大，内存占用都只有一个块。`finish_into_sink` 在写完尾部后把 sink（也就是缓冲区）交还给
调用方。

`hfile_rs::arrow::async_io` 为 Parquet 提供同样的能力：`parquet_to_hfile` 与
`hfile_to_parquet` 用 `ParquetRecordBatchStreamBuilder` 读、`AsyncArrowWriter` 写，并复用
`arrow::write` / `arrow::read` 的 mapping 与布局代码，因此异步与同步转换可以互相读取对方的
产物。详见 [docs/arrow-integration.zh-CN.md](docs/arrow-integration.zh-CN.md#异步转换)。

## 文档

* [docs/hfile-format.zh-CN.md](docs/hfile-format.zh-CN.md) —— 磁盘格式：块、校验和、编码、索引、
  文件信息、尾部、压缩
* [docs/read-write-flow.zh-CN.md](docs/read-write-flow.zh-CN.md) —— 读取器与写入器如何遍历文件
* [docs/arrow-integration.zh-CN.md](docs/arrow-integration.zh-CN.md) —— Arrow/Parquet 映射、两种
  布局、类型转换
* [docs/bulkload.zh-CN.md](docs/bulkload.zh-CN.md) —— bulk load 布局、检查清单与排错

## 测试

```bash
cargo test                       # 单测 + tests/ 下的集成测试
scripts/hbase-crosscheck.sh      # 可选：对真实 HBase jar 做交叉校验
```

`scripts/hbase-crosscheck.sh` 需要一套 HBase 2.5.x 环境，双向校验：用本 crate 读 HBase 写出的
HFile，以及用 HBase 的 `HFileTool` 读本 crate 写出的 HFile。所需环境变量见脚本头部注释。

## 许可证

Apache-2.0
