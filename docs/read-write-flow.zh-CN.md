# 读写流程

> [English](read-write-flow.md) | [简体中文](read-write-flow.zh-CN.md)

`HFileReader` 与 `HFileWriter` 各自如何遍历一个文件。两者的行为与 HBase 中同名类一一对应，
因此 HBase 的调用栈通常可以直接映射到本 crate。

## 读

`HFileReader::open` 对应 `HFile.createReader` + `HFileInfo.initMetaAndIndex`：

```text
1. 读取最后 4096 字节                 -> FileTrailer（偏移、计数、压缩算法）
2. 读取 FILEINF2 块                   -> tags/mvcc 标志、last key、统计信息
3. 读取 IDXROOT2 块                   -> 块索引树
4. 窥视第一个数据块                    -> DataBlockEncoding（来自块本身，而不是尾部）
5. 依据文件信息构造 DecodeOptions { includes_tags, includes_mvcc }
```

默认每个块都会校验校验和（`ReaderOptions::verify_checksums`）；`hfile info --no-checksum`
可以跳过，用于检查已损坏的文件。

### 顺序扫描

`iter()` 逐个块遍历数据区，等价于 `HFileBlock.BlockIterator`：

```text
first_data_block_offset
  -> 读块、解码 cell、逐个产出
  -> 下一偏移 = 当前偏移 + header.total_size()   （块头 + 负载 + 校验和）
  -> 越过 last_data_block_offset 即结束
```

完全不使用索引，所以即使索引损坏也能扫描。cell 按比较器顺序产出，并且按块惰性解码：内存
占用是一个块，而不是整个文件。

### 索引查找

`seek(flat_key)` 与 `get(row, family, qualifier)` 走索引树：

```text
在根索引中查找最后一个 <= 目标的 key
  -> 指向数据块   -> 解码并返回第一个 >= 目标的 cell
  -> 指向索引块   -> 在该块内重复（多层索引）
```

`get` 会拼出 `row/family/qualifier/timestamp=MAX/type=Maximum` 的扁平 key 再按前缀过滤，因此
拿到的是该 cell 的最新版本。由于数据块本身有序，一次 seek 每层最多读一个索引块，外加一个
数据块。

### 解码数据块

`decode_data_block` 按块 magic 分派：

* `DATABLK*` —— cell 首尾相接存放（`parse_raw_cells`），借助文件信息中的标志判断是否存在
  tags 长度与 mvcc vlong。
* `DATABLKE` —— 负载开头是 2 字节编码 id，随后由 `PREFIX`、`DIFF`、`FAST_DIFF`、
  `ROW_INDEX_V1` 解码器重建 cell。

## 写

`HFileWriter` 是流式写入器：cell 逐个追加，缓冲区满时落盘成块。

```text
create(path, options)      校验参数、截断文件、占用偏移 0
append(cell)               - 拒绝破坏比较器顺序的 cell
                           - 编码进当前块（编码器缓冲）
                           - 更新统计量（平均长度、最大 seq id、最大 cell 等）
                           - 缓冲达到 block_size 时 flush_data_block()
flush_data_block()         build_block(...) -> 追加写入文件，并以块首 key 与偏移记录一条索引
finish()                   刷出最后一块，写根索引、文件信息块、尾部，然后补齐到 4096 字节
```

注意事项：

* **顺序由调用方保证。** 一旦某个 cell 会破坏顺序，`append` 立刻返回错误，这比产出不可读的
  文件代价小。`write_hfile` 会先用 `compare_cells` 排序。
* **块大小是目标值，不是上限。** 编码缓冲达到 `block_size` 就刷块；单个超大 cell 仍会产生
  超大块。
* **`includes_mvcc` 默认为 `true`**，与 `HFileContext` 一致。它会写 `KEY_VALUE_VERSION = 1`
  并在每个 cell 后追加 mvcc vlong，这正是 HBase 对 bulk load 生成的 store file 的期望。只有
  明确知道读取端不需要时才关掉。
* **store file 元数据**（`MAX_SEQ_ID_KEY`、`MAJOR_COMPACTION_KEY`）默认写入，使产物看起来
  像 `StoreFileWriter` 的输出；`write_store_file_metadata = false` 可关闭。
* **索引最后写**，因此写索引时已经知道所有块的最终偏移。本写入器只产出单层索引；HBase 在
  根索引变大后才会切到多层。
* **`finish()` 必须调用。** 直接析构会留下没有尾部的文件，任何读取器都不会接受。

## 错误处理

`HFileError` 区分了调用方通常需要分别处理的情况：

| 变体 | 含义 |
|------|------|
| `Io` | 读写失败 |
| `Corrupt` | 校验和不匹配、magic 错误、尺寸不可能、负载无法解码 |
| `Unsupported` | 合法但本 crate 未实现的特性（如 `LZ4`、压缩的 tags） |
| `InvalidArgument` | 调用方错误：未排序的 cell、未知编码名、空输入 |
| `Proto` | 尾部或文件信息 protobuf 损坏 |
| `Arrow` | Parquet/Arrow 转换失败 |

损坏的块以 `Corrupt` 报出并指出失败的校验分块；凡是"另一种选择是静默返回错误数据"的场景，
一律用 `Unsupported`。
