# HFile v3 磁盘格式

> [English](hfile-format.md) | [简体中文](hfile-format.zh-CN.md)

本文只描述本 crate 实现的那部分 HBase HFile 格式。字段名沿用
`org.apache.hadoop.hbase.io.hfile` 中的命名，便于与 HBase 源码对照。

## 文件整体布局

```text
偏移 0                         第一个数据块               "DATABLK*" / "DATABLKE"
        ...                    更多数据块                 通过 prevBlockOffset 反向串成链表
last_data_block_offset         最后一个数据块
load_on_open_data_offset       根索引块                   "IDXROOT2"
file_info_offset               文件信息块                 "FILEINF2"
文件大小 - 4096                固定文件尾部                "TRABLK\"$"
```

`load_on_open_data_offset` 之前的区域属于数据区，只能通过索引到达；从
`load_on_open_data_offset` 到文件尾属于 "load on open" 区（对应
`HFileInfo.initMetaAndIndex`）。

## 块结构

每个块以 33 字节块头开头（`HFileBlock`）：

| 偏移 | 长度 | 字段 |
|-----:|-----:|------|
| 0 | 8 | 块类型 magic，如 `DATABLK*`、`IDXROOT2`、`FILEINF2` |
| 8 | 4 | `onDiskSizeWithoutHeader` —— 负载 + 校验和 |
| 12 | 4 | `uncompressedSizeWithoutHeader` —— 仅负载 |
| 16 | 8 | `prevBlockOffset`，同类型前一个块的偏移（没有则为 `-1`） |
| 24 | 1 | `checksumType`：`0` NULL、`1` CRC32、`2` CRC32C |
| 25 | 4 | `bytesPerChecksum`（HBase 默认 16384） |
| 29 | 4 | `onDiskDataSizeWithHeader` —— 块头 +（可能被压缩的）负载 |
| 33 | N | 负载；当尾部指定了压缩算法时为压缩后的内容 |

校验和紧跟在负载之后：

```text
分块数 = max(1, ceil(onDiskDataSizeWithHeader / bytesPerChecksum))
块     = 块头(33) + 负载 + 分块数 * 4 字节 CRC32C（大端）
```

校验和覆盖**包含块头**的整块内容，逐分块计算；并且始终针对落盘字节计算，也就是压缩后的
负载。

`prevBlockOffset` 把数据块串成一条反向链表。读取并不需要它，但 HBase 在 seek 之后把块放入
缓存时会用到。

## 数据块内的 cell

数据块内是一串紧挨着的 `KeyValue`，没有长度前缀。单个 cell 的布局：

```text
rowLen(2) row  familyLen(1) family  qualifier  timestamp(8, 大端) type(1) valueLen(4) value
[tagsLen(2) tags]     仅当文件带 hfile.MAX_TAGS_LEN
mvcc(vlong)           仅当 KEY_VALUE_VERSION == 1（即 includes_mvcc）
```

同一个块内的 cell 按 `CellComparator` 排序（row、family、qualifier 升序；timestamp 与 type
降序）。末尾的 `mvcc` 就是 `hfile2arrow` 输出的 `seq`。

编码块在负载前放 2 字节大端编码 id，cell 布局与上面完全不同，见下文。

## 数据块编码

| id | `DataBlockEncoding` | cell 布局 | 本 crate 支持 |
|---:|---------------------|-----------|--------------|
| 0 | `NONE` | 上文所述的原始 cell，块 magic 为 `DATABLK*` | 读 + 写 |
| 2 | `PREFIX` | 当前 key 与上一个 key 共享前缀，只存差异 | 读 + 写 |
| 3 | `DIFF` | 共享前缀 + 独立的 timestamp/type/value 去重 | 读 |
| 4 | `FAST_DIFF` | 与 `DIFF` 类似，解码更快 | 读 |
| 7 | `ROW_INDEX_V1` | 原始 cell + 每个 row 的偏移索引（HBase 2 默认） | 读 + 写 |

编码**不**在尾部记录。HBase 把它放在文件信息的 `DATA_BLOCK_ENCODING` 键里
（`HFileDataBlockEncoderImpl.createFromFileInfo`）；忽略该键的读取器会把 `DATABLKE` 块当作
普通 cell 解析。`NONE` 从不会被记录。

## 索引块

根索引是一个 protobuf 消息（`HFileBlockIndex.BlockIndex`）：

```text
entry[]  { key        : 被索引块的第一个 key
           offset     : 块偏移
           onDiskSize : 含块头的块长度，用于预读 }
firstKey, numEntries
```

本写入器产出**单层**索引：每个数据块一条记录，直接指向数据块，
`numDataIndexLevels = 1`。读取器也支持多层索引（`numDataIndexLevels > 1`，根记录指向
`IDXINTE2` 块），这是 HBase 在根索引本身变大后才会使用的方式。

## 文件信息

文件信息块是一个字符串键到字节值的 protobuf map。本写入器会写：

| 键 | 值 |
|----|-----|
| `KEY_VALUE_VERSION` | `1` —— cell 尾部带 mvcc vlong |
| `MAX_SEQ_ID_KEY` | 文件中最高的 sequence id |
| `MAJOR_COMPACTION_KEY` | `0` —— `StoreFileWriter` 的标记 |
| `MAX_MEMSTORE_TS_KEY` | 最高的 memstore 时间戳（与上者相同） |
| `hfile.AVG_KEY_LEN`、`hfile.AVG_VALUE_LEN` | 平均长度，用于块尺寸决策 |
| `hfile.CREATE_TIME_TS` | 创建时间（毫秒） |
| `hfile.KEY_OF_BIGGEST_CELL`、`hfile.LEN_OF_BIGGEST_CELL` | 最大 cell，用于块尺寸 |
| `hfile.LASTKEY` | 最后一个 cell 的 key，读取器无需扫描即可定位末尾 |
| `hfile.MAX_TAGS_LEN`、`hfile.TAGS_COMPRESSED` | cell 带 tags 时写入（`TAGS_COMPRESSED = 0`） |
| `DATA_BLOCK_ENCODING` | 编码名，非 `NONE` 时才写 |

`hfile2arrow` 用 `KEY_VALUE_VERSION` 判断 cell 是否带 mvcc，用 `hfile.MAX_TAGS_LEN`（HBase 在
带 tags 时写入）判断是否带 tags。

## 文件尾部

文件最后 4096 字节（`FixedFileTrailer`）：

```text
magic "TRABLK\"$"             8
varint 长度 + FileTrailerProto
零填充
落盘的版本号                    4   (minor << 24) | major
```

`FileTrailerProto` 携带 `fileInfoOffset`、`loadOnOpenDataOffset`、
`uncompressedDataIndexSize`、`totalUncompressedBytes`、`dataIndexCount`、`metaIndexCount`、
`entryCount`、`numDataIndexLevels`、`firstDataBlockOffset`、`lastDataBlockOffset`、
`comparatorClassName`、`compressionCodec`，以及可选的 `encryptionKey`。

本 crate 写入 `major = 3`、`minor = 3`（HBase 2.x 读出来显示为 `version=3.3`），并使用传统
比较器名 `org.apache.hadoop.hbase.KeyValue$KVComparator`，与 HBase 写普通 `KeyValue` 时一致。

## 压缩

`Compression.Algorithm` 的序数是**声明顺序**而非字母顺序，且 `BZIP2` 是插在中间加入的：

| 序号 | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 |
|------|---|---|---|---|---|---|---|---|---|
| 算法 | LZO | GZ | NONE | SNAPPY | LZ4 | BZIP2 | ZSTD | LZMA | BROTLI |

把 `ZSTD` 写成序号 `5` 会让 HBase 选用 BZIP2 编解码器，随后以
`BZip2DummyDecompressor.finished` 失败 —— 这是本 crate 曾经踩过的坑，现在有回归测试。

`GZ` 负载就是普通 gzip 容器。`ZSTD` 负载取决于是哪个实现：

```text
raw        <zstd 帧>                                              Hadoop ZStandardCodec（HBase 默认）
hbase      <未压缩长度:4><帧长度:4><zstd 帧>                       hbase-compression-zstd（纯 Java）
```

两者互不兼容，所以必须在写入时选定（`WriterOptions::zstd_framing`，CLI 为
`--zstd-framing`）。读取时按负载自行判别：zstd 帧总是以 `28 b5 2f fd` 开头，而 `hbase`
框架自带自描述信息（重复了块的未压缩长度）。

## 有意省略的部分

* bloom filter（`BLMFBLK2`）、meta block，以及除索引之外的 load-on-open 扩展
* 加密（`Encryption.Context`）
* `LZO`、`SNAPPY`、`LZ4`、`BZIP2`、`LZMA`、`BROTLI` —— 一律报"不支持"，而不是用错误的
  框架去解
* 写入多层索引，以及 `DIFF` / `FAST_DIFF` 编码器
