# Arrow / Parquet 集成

> [English](arrow-integration.md) | [简体中文](arrow-integration.zh-CN.md)

HFile 没有 schema：它存的是 `(row, family, qualifier, timestamp, type, value)` 的 cell。Arrow
是有类型的。两者之间的桥梁是 `Mapping`，而根据"谁掌握 schema"，它有两种形态。

## 两种输入布局

### Raw 布局（一行一个 cell）

`hfile2arrow` 在不给 `--schema` 时，每个 cell 输出一行，schema 固定：

| 列 | 类型 | 含义 |
|----|------|------|
| `__rowkey` | `Binary` | 行键 |
| `__family` | `Binary` | 列族 |
| `__qualifier` | `Binary` | 列限定符 |
| `__value` | `Binary` | 原样存储的值；删除标记为 `NULL` |
| `__timestamp` | `Int64` | HBase 时间戳 |
| `__cell_type` | `UInt8` | `KeyValue` 类型字节 |

该布局对 row/family/qualifier/value/timestamp/type 无损，但**不**携带 mvcc/sequence id，值本身
仍是不可解释的字节。回写时完全不传 `--row-key`，或传 `--row-key __rowkey`，两者都识别为 raw
布局。

### 宽表布局（一行一个 HBase 行）

给了 schema 就得到一行一个 HBase 行、一列一个 qualifier 的 Parquet，可以直接被 SQL 引擎使用：

```text
id   Int64   -> 行键
name Utf8    -> cell cf:name       （族来自 --family，或列名里的 "cf:name"）
ts   Int64   -> HBase 时间戳        （通过 --timestamp-column）
```

回写时，Arrow schema（用 `--schema-out` 保存）告诉转换器哪些列是行键组成部分、哪一列是时间戳、
每个 cell 原本是什么类型。行键列全为 null 的行会被跳过，cell 值为 null 的也跳过。

## Mapping

```json
{
  "row_key": ["tenant", "id"],
  "row_key_separator": "\u0000",
  "family": "cf",
  "timestamp_column": "ts",
  "timestamp": null,
  "value_encoding": "text"
}
```

| 字段 | 默认值 | 含义 |
|------|--------|------|
| `row_key` | `[]` | 拼成行键的列；为空表示"使用 `__rowkey` 列" |
| `row_key_separator` | `"\u0000"` | 行键各部分之间的字节。若值中可能出现 NUL，改用 `"\u0001"` 之类 |
| `family` | `null` | 列名没有 `family:` 前缀时使用的族 |
| `timestamp_column` | `null` | 存放时间戳的列（必须是整数类型） |
| `timestamp` | `null` | 固定时间戳；两者都未指定时 cell 取 `i64::MAX`（`HConstants.LATEST_TIMESTAMP`） |
| `value_encoding` | `"text"` | `"text"` 或 `"binary"` |

名为 `cf:name` 的列总是落到族 `cf`、qualifier `name`，优先于 `family`。

## 类型映射

`Utf8`、`LargeUtf8`、`Binary`、`LargeBinary` 一律按字节原样存储，因此两种 `value_encoding`
都能无损往返。

| Arrow 类型 | `text` | `binary` |
|------------|--------|----------|
| `Boolean` | `true` / `false` | 1 字节 `0`/`1` |
| `Int8`..`Int64`、`UInt8`..`UInt64` | 十进制字符串 | 大端，原生宽度 |
| `Float32`、`Float64` | Rust `Display`（`1.5`、`NaN`、`inf`） | IEEE-754 位模式，大端 |
| `Date32`、`Date64` | 距 epoch 的天数/毫秒数，十进制 | 大端 `i32`/`i64` |
| `Timestamp(s|ms|us|ns)` | 该单位下的原始数值，十进制 | 大端 `i64` |
| `Decimal128(p, s)` | 未缩放值，十进制 | 大端 `i128` |
| `Utf8`、`LargeUtf8`、`Binary`、`LargeBinary` | 原始字节 | 原始字节 |

几个省时间的要点：

* 行键各部分与时间戳**恒按大端二进制**序列化，与 `value_encoding` 无关；读回时同样按二进制
  解码。这样数值型行键无损且无歧义，也正是必须用 `--schema` 才能还原其 Arrow 类型的原因。
* `text` 对 `Float32`/`Float64` 在个别边界情形下不等价（`-0.0`、部分非规格化数），也会丢掉
  `Decimal128` 的 scale；在意这些时用 `binary`。
* 这里的"时间戳"是 cell 的 HBase 时间戳，不是事件的 Arrow 时间；两者通常一致，但没有任何
  强制关系。
* 嵌套类型（`List`、`Struct`、`Map`）、`Float16` 和字典编码列都会被拒绝，报
  `invalid argument: unsupported Arrow type ...`。

## CLI 示例

```bash
# 宽表布局；`--row-key` 接收逗号分隔的列名，schema 保存下来供回程使用
hfile arrow2hfile hits.parquet --out t.hfile \
    --row-key tenant,id --family cf \
    --timestamp-column ts --value-encoding binary --schema-out t.schema.json

hfile hfile2arrow t.hfile --out hits2.parquet \
    --schema t.schema.json --row-key tenant,id --family cf

# raw 布局：任意 HFile，无需 schema
hfile hfile2arrow store.hfile --out cells.parquet --limit 1000
hfile arrow2hfile cells.parquet --out copy.hfile --encoding PREFIX --codec gz
```

行键分隔符没有对应的命令行开关：`--row-key a,b` 恒定用单个 NUL 字节连接。当各部分可能含 NUL
或想要可读的分隔符时，请使用 mapping 文件：

```bash
cat > mapping.json <<'JSON'
{ "row_key": ["tenant", "id"], "row_key_separator": "|", "family": "cf" }
JSON
hfile arrow2hfile hits.parquet --out t.hfile --mapping mapping.json --timestamp-column ts
```

`hfile2arrow` 与 `arrow2bulkload` 也读取同一个 mapping 文件（`--mapping`），因此复合行键只需
描述一次。

由于宽表回写需要行键，`--row-key` 列出的列必须出现在回程的 Parquet 中；如果原始时间戳并不
重要，可以用固定的 `--timestamp` 代替 `--timestamp-column`。

## 排序

HFile 必须按 `CellComparator` 有序。`arrow2hfile` 会在写之前于内存中排序，因此任何顺序的
Parquet 都能接受，代价是整个文件被缓冲。数据大到放不进内存时，请预先按
`(row, family, qualifier, ts desc)` 排序，再用库 API（`HFileWriter::append`）流式写入。
