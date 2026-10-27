# Bulk load（批量导入）

> [English](bulkload.md) | [简体中文](bulkload.zh-CN.md)

`LoadIncrementalHFiles` 接收一个形如"表 store file 目录树"的目录，校验（可选地切分）其中的
HFile，然后直接让表引用这些文件而无需重写数据。本 crate 产出的正是这种布局。

## 生成目录

```bash
hfile arrow2bulkload hits.parquet \
    --dir /tmp/bulk/hits \
    --family cf \
    --row-key tenant,id \
    --timestamp-column ts \
    --encoding ROW_INDEX_V1 \
    --codec zstd --zstd-framing hbase \
    --file-name 0001.hfile
```

行键各部分用单个 NUL 字节连接；需要其它 `row_key_separator` 时传 `--mapping mapping.json`
（见 [arrow-integration.zh-CN.md](arrow-integration.zh-CN.md#mapping)）。

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--dir` | – | 交给 `LoadIncrementalHFiles` 的目录 |
| `--family` | – | 列族名**同时也是**子目录名，必须已存在于表中 |
| `--file-name` | `<family>.hfile` | 族目录内的文件名；保留 `.hfile` 后缀 |
| `--mapping` | – | JSON mapping 文件，见 [arrow-integration.zh-CN.md](arrow-integration.zh-CN.md) |
| `--encoding` | `NONE` | bulk load 通常用 `PREFIX` 或 `ROW_INDEX_V1` 更合适 |
| `--codec` | `none` | 必须是集群能读的算法 |
| `--zstd-framing` | `raw` | Hadoop 编解码器用 `raw`，`hbase-compression-zstd` 用 `hbase` |
| `--block-size` | 集群默认 | 目标数据块大小（字节） |

产物：

```text
/tmp/bulk/hits/
├── _SUCCESS
└── cf/
    └── 0001.hfile
```

`_SUCCESS` 是必需的：`LoadIncrementalHFiles` 把该目录当作作业输出，缺少它就直接拒绝执行。

## 执行导入

```bash
# 目录必须位于集群的文件系统上，不能只在本机
hdfs dfs -put -f /tmp/bulk/hits /tmp/bulk/hits

hbase org.apache.hadoop.hbase.tool.LoadIncrementalHFiles /tmp/bulk/hits hits
```

`LoadIncrementalHFiles` 会先进入 "bulk load prepare" 阶段，读取每个 HFile 并根据行键计算 region
切分，因此需要：

* 与写文件时一致的 `hbase.io.compress.zstd.codec`（或其它压缩算法）配置 —— 否则 prepare 阶段
  读文件时就会失败，远在真正导入之前；
* 目标 HBase 版本能读的 HFile（本 crate 写的是 HFile v3，正是 HBase 2.x 期望的格式）。

首次使用如果还想让工具顺带建表，可以加 `-Dcreate.table=yes`。

## 检查清单

1. **列族已存在。** `--family` 必须是目标表的列族，否则 store file 会落到错误的目录，导入报
   "wrong family name"。
2. **压缩算法匹配。** `--codec`/`--zstd-framing` 必须是每个 RegionServer 都能读的；不确定就用
   `--codec none`。
3. **排序。** `arrow2bulkload` 写之前会按 `CellComparator` 排序，因此 Parquet 可以任意顺序到达。
   行键各部分按 `--row-key` 给出的顺序拼接。
4. **一次一个族。** 目录里可以放多个族子目录，但每个文件只能包含单一族的 cell。按族各跑一次
   命令、指向同一个 `--dir` 即可（`_SUCCESS` 会被覆盖，没有影响）。
5. **时间戳。** 不给 `--timestamp-column` 时每个 cell 都是 `i64::MAX`
   （`HConstants.LATEST_TIMESTAMP`），也就是会盖过之后用真实时间戳写入的任何数据。数据本身带
   时间戳时请显式指定。
6. **sequence id。** `MAX_SEQ_ID_KEY` 由 cell 的 `seq_id` 推导，来自 Parquet 的数据默认为 `0`
   （raw 布局里没有 `__seq` 列）。`LoadIncrementalHFiles` 在导入过程中会重写 store file 元数据，
   因此这只影响手工拷贝文件到位的场景。

## 导入前自检

```bash
# 用本项目的读取器
hfile info /tmp/bulk/hits/cf/0001.hfile --dump --limit 5

# 用 HBase 自己的工具（需要把 HBase jar 放进 classpath）
hbase org.apache.hadoop.hbase.io.hfile.HFileTool read /tmp/bulk/hits/cf/0001.hfile | head
```

`scripts/hbase-crosscheck.sh` 把上面两个方向自动化了；目录布局本身由 `tests/cli.rs` 中的
`bulkload_writes_the_layout_expected_by_load_incremental_hfiles` 覆盖。

## 常见错误

| 报错 | 原因 |
|------|------|
| `Bulk load file ... not a valid HFile` | 文件被截断 —— 写入器未调用 `finish()` 就被析构，或上传中断 |
| `Can't read hfile` / `Problem reading data index` | 压缩算法不匹配（见第 2 条），或 `--file-name` 后缀不对 |
| 导入后 `NoSuchColumnFamilyException` | `--family` 与表定义不一致 |
| prepare 阶段 `RetriesExhaustedWithDetailsException` | 目标表的 region 尚未上线 |
| 值看起来全是 `\x00\x00\x00\x00...` | 行键或时间戳列按 `text` 而非 `binary` 解码，见 [arrow-integration.zh-CN.md](arrow-integration.zh-CN.md) |
