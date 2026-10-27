# HFile v3 磁盘格式

> [English](hfile-format.md) | [简体中文](hfile-format.zh-CN.md)

本文只描述本 crate 实现的那部分 HBase HFile 格式。字段名沿用
`org.apache.hadoop.hbase.io.hfile` 中的命名，便于与 HBase 源码对照。

## 文件整体布局

```text
偏移 0                         第一个数据块               "DATABLK*" / "DATABLKE"
        ...                    更多数据块                 通过 prevBlockOffset 反向串成链表
        ...                    bloom 分块                 "BLMFBLK2"
        ...                    meta block                 "METABLKc"
last_data_block_offset         最后一个数据块
load_on_open_data_offset       根索引块                   "IDXROOT2"
                               meta 索引块                "IDXROOT2"
file_info_offset               文件信息块                 "FILEINF2"
                               bloom filter 元数据        "BLMFMET2"
文件大小 - 4096                固定文件尾部                "TRABLK\"$"
```

`load_on_open_data_offset` 之前的区域属于数据区，只能通过索引到达；从
`load_on_open_data_offset` 到文件尾属于 "load on open" 区（对应
`HFileInfo.initMetaAndIndex`）。该区是按顺序读的一串块：根索引、meta 索引、文件信息，
以及随后的"附加"块（例如 bloom filter 元数据）。

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

`SNAPPY` 负载就是 `SnappyCodec` 写出的形式，即 Hadoop `BlockCompressorStream` 的块布局：

```text
<未压缩长度:4 BE><压缩长度:4 BE><raw snappy 字节>...
```

头部是该块解压后的字节数。分块个数并不落盘，读取方只需不断消费"长度 + 数据"直到凑满
这个字节数。HFile 一个块只做一次 write 加一次 `finish()`，因此小于
`bufferSize - (bufferSize/6 + 32)`（默认 256 KiB 缓冲即 218422 字节）的块正好一对，更大的
块会被切成多对。分块数据本身是 raw snappy，而不是 snappy 的 framing 格式。

写出这种布局的编解码器有两个，输出可以互换：Hadoop 原生的
`org.apache.hadoop.io.compress.SnappyCodec`，以及 HBase 纯 Java 的
`org.apache.hadoop.hbase.io.compress.xerial.SnappyCodec`（`hbase.io.compress.snappy.codec`，
它刻意与前者保持一致）。与 `ZSTD` 不同，这里不需要选择框架。

## Meta block

`HFile.Writer#appendMetaBlock(name, payload)` 把一段不透明字节存在名字下。这些块以
`METABLKc` 紧挨在数据块之后按名字排序写出，再由 meta 索引（第二个 `IDXROOT2` 块，
`meta_index_count` 条 `(offset:long, onDiskSize:int, key)`，布局与数据根索引完全一致）
记录名字到位置的映射。该索引块总是写出，即使一条都不含；`FixedFileTrailer.metaIndexCount`
也只统计 `METABLKc`，不含 bloom filter 元数据。HBase 的 `getMetaBlock` 返回首个 key
小于等于目标名字的条目，因此查一个不存在的名字会拿到相邻的块；本 crate 则返回 `None`。

## Bloom filter

`ROW` bloom filter 写在三处：

* 数据区里每个分块一个 `BLMFBLK2` 块，负载就是裸的位图；
* load on open 区最后一个块 `BLMFMET2`，保存公共参数与"每块 `(firstKey, offset, size)`"
  的单层索引；
* 文件信息键 `BLOOM_FILTER_TYPE` = `ROW`。

`BLMFMET2` 的负载（`CompoundBloomFilterWriter.MetaWriter`）为：

```text
<int VERSION=3><long totalByteSize><int hashCount><int hashType>
<long totalKeyCount><long totalMaxKeys><int numChunks>
<vint 比较器名长度=0><0 字节>                              # ROW 不需要比较器
numChunks * ( <long offset><int onDiskSize><vint 键长><firstKey> )
```

一行归属哪个分块由比较器顺序决定（`firstKey` 是该分块的第一行）；计算时用 murmur2
（`h1 = hash(row, 0)`、`h2 = hash(row, h1)`、`hashType = 1`），对
`i` 取 `abs((h1 + i * h2) % (byteSize * 8))`，共 `hashCount` 次。分块大小由
`io.storefile.bloom.block.size`（128 KiB 位图）决定，默认 1% 误差率下约容纳 109382 个键、
7 个哈希函数；如果某个分块最后远没装满，会不断折半直到 `maxKeys <= 2 * keyCount`，
由于所有探测位置都是 2 的幂取模，折半不会丢失任何命中。

有两点决定兼容性，也正是本 crate 没有直接使用通用 bloom filter crate 的原因：位图是**扁平**的
（不是分块式），哈希用的是 HBase 自己的 `MurmurHash`（末尾不足 4 字节时按符号扩展移位）。
`murmur2` crate 是标准 `MurmurHash2`，长度是 4 的倍数时与 HBase 完全一致，但末尾字节按无符号
扩展：`[0x80, 0x81, 0x82, 0x83, 0x84]` 在 HBase 是 -1368132752，在它那里是 -1237960112。
探测位置是 `hash % bitSize`，哈希不同就意味着落在不同的位，所以末尾部分由本 crate 自己拼装
（见 `src/hbase/bloom.rs`）。`ROWCOL` 过滤器的哈希键是拼出来的
`row/family/qualifier` 单元键，本 crate 未实现。

## 有意省略的部分

* `ROWCOL` 与 `ROWPREFIX_FIXED_LENGTH` bloom filter，以及非 murmur 的哈希类型
* 加密（`Encryption.Context`）
* `LZO`、`LZ4`、`BZIP2`、`LZMA`、`BROTLI` —— 一律报"不支持"，而不是用错误的
  框架去解
* 写入多层索引，以及 `DIFF` / `FAST_DIFF` 编码器
