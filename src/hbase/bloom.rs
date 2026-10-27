//! HBase bloom filters: a `BLMFMET2` metadata block plus `BLMFBLK2` chunk blocks.
//!
//! An HFile v3 bloom filter is made of three pieces:
//!
//! ```text
//! [bloom chunk 0][bloom chunk 1] ...        BLMFBLK2 blocks, interleaved with the data blocks
//! ... root index, meta index, file info
//! [bloom metadata]                          BLMFMET2 block, last load-on-open block
//! ```
//!
//! * a chunk payload is the bare bit array (no header), so a chunk's size is
//!   `uncompressedSizeWithoutHeader` of its block;
//! * the metadata block holds the parameters shared by all chunks (version, hash type, hash
//!   count) plus a single level index of `(firstKey, offset, size)` per chunk;
//! * the file info key `BLOOM_FILTER_TYPE` records the bloom type (`ROW`).
//!
//! ## Why not `fastbloom`
//!
//! HBase's filter is a *flat* bit array whose positions are
//! `abs((h1 + i * h2) % bitSize)` with `h1 = hash(key, 0)`, `h2 = hash(key, h1)` over a 32 bit
//! murmur2. `fastbloom` implements a *blocked* filter (the high bits of the hash select a 512 bit
//! block, a different hash family produces the bits), so its arrays are not interchangeable with
//! HBase's: HBase would answer "not present" for rows that are in the file and skip their blocks.
//! [`murmur_hash`] and [`BloomChunk`] below therefore port HBase's `MurmurHash` and
//! `BloomFilterChunk` directly.
//!
//! ## Why not the `murmur2` crate
//!
//! `murmur2::murmur2` is stock `MurmurHash2`, which is what HBase's body and finalisation are, so
//! the two agree for every key whose length is a multiple of four. They disagree on the leftover
//! bytes: HBase reads them as Java `byte`s (signed, and `^=`ed into the hash), the crate as
//! `byte as u32` (`|`ed into a separate word). For `[0x80, 0x81, 0x82, 0x83, 0x84]` that is
//! -1368132752 against -1237960112. A wrong hash means wrong bits, and a bloom filter that says
//! "not present" for a stored row makes HBase skip the block that holds it, so the tail is
//! assembled by hand here. The crate stays a dev dependency and is exercised as the counter
//! example in `tests::the_murmur2_crate_parts_ways_on_the_signed_tail`.
//!
//! Only `BloomType.ROW` is written; `ROWCOL` (whose hash key is a synthetic
//! `row/family/qualifier` cell key) is reported as unsupported instead of emitting a filter that
//! HBase would query with a different key.

use crate::error::{HFileError, Result, corrupt, unsupported};
use crate::hbase::kv::{read_compressed_int, write_compressed_int};

/// `Hash.MURMUR_HASH`, the default `hbase.hash.type`.
pub const MURMUR_HASH: i32 = 1;

/// `CompoundBloomFilterBase.VERSION`, the only bloom metadata version HBase writes.
pub const BLOOM_META_VERSION: i32 = 3;

/// `io.storefile.bloom.error.rate` default.
pub const DEFAULT_ERROR_RATE: f32 = 0.01;
/// `io.storefile.bloom.block.size` default: the target size of one chunk's bit array.
pub const DEFAULT_CHUNK_SIZE: usize = 128 * 1024;
/// `io.storefile.bloom.max.fold` default, also `MAX_ALLOWED_FOLD_FACTOR`.
pub const DEFAULT_MAX_FOLD: u32 = 7;

/// `BloomType`, restricted to what this crate writes and reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BloomType {
    /// No bloom filter in the file.
    #[default]
    None,
    /// One entry per row.
    Row,
}

impl BloomType {
    pub fn name(self) -> &'static str {
        match self {
            BloomType::None => "NONE",
            BloomType::Row => "ROW",
        }
    }

    pub fn parse_name(name: &str) -> Result<Self> {
        Ok(match name.to_ascii_uppercase().as_str() {
            "NONE" => BloomType::None,
            "ROW" => BloomType::Row,
            "ROWCOL" | "ROWPREFIX_FIXED_LENGTH" => {
                return unsupported(format!(
                    "bloom type '{name}' is not implemented; only NONE and ROW are"
                ));
            }
            other => {
                return Err(HFileError::InvalidArgument(format!(
                    "unknown bloom type '{other}' (expected NONE or ROW)"
                )));
            }
        })
    }
}

/// Tuning for the bloom filter written by [`crate::hbase::writer::HFileWriter`].
#[derive(Debug, Clone, PartialEq)]
pub struct BloomOptions {
    /// Target false positive rate (`io.storefile.bloom.error.rate`).
    pub error_rate: f32,
    /// Target bit array size of one chunk (`io.storefile.bloom.block.size`).
    pub chunk_size: usize,
    /// How often a chunk may be halved when it holds far fewer keys than it could
    /// (`io.storefile.bloom.max.fold`).
    pub max_fold: u32,
    /// Hash function (`hbase.hash.type`); only `murmur` is implemented.
    pub hash_type: i32,
}

impl Default for BloomOptions {
    fn default() -> Self {
        BloomOptions {
            error_rate: DEFAULT_ERROR_RATE,
            chunk_size: DEFAULT_CHUNK_SIZE,
            max_fold: DEFAULT_MAX_FOLD,
            hash_type: MURMUR_HASH,
        }
    }
}

// ---------------------------------------------------------------------------
// HBase hashing
// ---------------------------------------------------------------------------

/// `org.apache.hadoop.hbase.util.MurmurHash`: murmur2 mixed as 32 bit Java ints.
///
/// The body and the finalisation are stock `MurmurHash2`, which is also what `murmur2::murmur2`
/// implements. The leftover bytes are not stock: HBase reads them as Java `byte`s, and those are
/// *signed*, so `data[..] << 16` shifts the sign extension into the hash instead of the byte. It
/// also `^=`s the three terms into `h` (they overlap once sign extended), where a zero extending
/// implementation `|`s disjoint bytes into a separate word.
///
/// `murmur2::murmur2` builds its tail word with `byte as u32`, so it agrees with HBase exactly
/// while no leftover byte has the high bit set, and silently produces a different hash as soon as
/// one does:
///
/// ```text
/// key                  HBase         murmur2::murmur2(.., 0)
/// 8081828384           -1368132752   -1237960112
/// ```
///
/// A bloom probe is `hash % bitSize`, so a different hash means different bits: HBase would
/// answer "not present" for rows that are in the file and skip their blocks. That is why the tail
/// is assembled here rather than delegated to the crate; see
/// `tests::the_murmur2_crate_parts_ways_on_the_signed_tail`, which pins down exactly where the
/// crate agrees and where it does not.
pub fn murmur_hash(data: &[u8], seed: i32) -> i32 {
    const M: i32 = 0x5bd1_e995;
    let length = data.len() as i32;
    let mut h = seed ^ length;

    let words = data.len() / 4;
    for i in 0..words {
        let b = &data[i * 4..i * 4 + 4];
        // Same byte order as HBase's `k = get(3) << 8 | get(2) & 0xff ...`.
        let mut k = (b[3] as i8 as i32).wrapping_shl(8);
        k |= b[2] as i32;
        k = k.wrapping_shl(8);
        k |= b[1] as i32;
        k = k.wrapping_shl(8);
        k |= b[0] as i32;
        k = k.wrapping_mul(M);
        k ^= (k as u32 >> 24) as i32;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }

    // Java: `case 3: h ^= data[length - 3] << 16; ...` with signed bytes, so 0x84 as the last
    // leftover byte contributes 0xFFFFFF84 and not 0x00000084.
    let tail = &data[words * 4..];
    if !tail.is_empty() {
        if tail.len() >= 3 {
            h ^= (tail[2] as i8 as i32).wrapping_shl(16);
        }
        if tail.len() >= 2 {
            h ^= (tail[1] as i8 as i32).wrapping_shl(8);
        }
        h ^= tail[0] as i8 as i32;
        h = h.wrapping_mul(M);
    }

    h ^= (h as u32 >> 13) as i32;
    h = h.wrapping_mul(M);
    h ^= (h as u32 >> 15) as i32;
    h
}

/// The bit positions `BloomFilterChunk.setHashLoc` / `BloomFilterUtil.contains` probe.
struct HashPositions {
    composite: i32,
    hash2: i32,
    remaining: i32,
    bit_size: i64,
}

impl Iterator for HashPositions {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        // Java: `Math.abs((h1 + i * h2) % bitSize)`; the sum wraps as an int, the modulo does not.
        let loc = (self.composite as i64 % self.bit_size).unsigned_abs() as usize;
        self.composite = self.composite.wrapping_add(self.hash2);
        Some(loc)
    }
}

fn positions(key: &[u8], hash_type: i32, hash_count: i32, bit_size: i64) -> Result<HashPositions> {
    if hash_type != MURMUR_HASH {
        return unsupported(format!(
            "bloom hash type {hash_type} is not implemented (only MURMUR_HASH = {MURMUR_HASH})"
        ));
    }
    if bit_size <= 0 {
        return corrupt("bloom filter has an empty bit array");
    }
    let hash1 = murmur_hash(key, 0);
    let hash2 = murmur_hash(key, hash1);
    Ok(HashPositions {
        composite: hash1,
        hash2,
        remaining: hash_count,
        bit_size,
    })
}

// ---------------------------------------------------------------------------
// One chunk
// ---------------------------------------------------------------------------

/// `org.apache.hadoop.hbase.util.BloomFilterChunk`: the parameters plus the bit array.
#[derive(Debug, Clone)]
pub struct BloomChunk {
    byte_size: u64,
    hash_count: i32,
    hash_type: i32,
    key_count: i32,
    max_keys: i32,
    bits: Vec<u8>,
}

impl BloomChunk {
    /// `BloomFilterUtil.createBySize`.
    pub fn create_by_size(
        byte_size_hint: usize,
        error_rate: f32,
        hash_type: i32,
        max_fold: u32,
    ) -> Result<Self> {
        if hash_type != MURMUR_HASH {
            return unsupported(format!(
                "bloom hash type {hash_type} is not implemented (only MURMUR_HASH = {MURMUR_HASH})"
            ));
        }
        if !(error_rate > 0.0 && error_rate < 1.0) {
            return Err(HFileError::InvalidArgument(format!(
                "bloom error rate must be between 0 and 1, got {error_rate}"
            )));
        }
        let byte_size = compute_foldable_byte_size(byte_size_hint as i64 * 8, max_fold)?;
        let bit_size = byte_size as i64 * 8;
        let max_keys = ideal_max_keys(bit_size, error_rate);
        let hash_count = optimal_function_count(max_keys, bit_size)?;
        let max_keys = compute_max_keys(bit_size, error_rate, hash_count);
        Ok(BloomChunk {
            byte_size,
            hash_count,
            hash_type,
            key_count: 0,
            max_keys,
            bits: vec![0u8; byte_size as usize],
        })
    }

    /// `BloomFilterChunk.createAnother`: same parameters, zeroed bits, no keys.
    fn create_another(&self) -> Self {
        BloomChunk {
            byte_size: self.byte_size,
            hash_count: self.hash_count,
            hash_type: self.hash_type,
            key_count: 0,
            max_keys: self.max_keys,
            bits: vec![0u8; self.byte_size as usize],
        }
    }

    pub fn add(&mut self, key: &[u8]) {
        if self.key_count >= self.max_keys {
            return;
        }
        for pos in positions(
            key,
            self.hash_type,
            self.hash_count,
            self.byte_size as i64 * 8,
        )
        .unwrap_or_else(|_| unreachable!("parameters are validated at construction"))
        {
            self.bits[pos >> 3] |= 1 << (pos & 7);
        }
        self.key_count += 1;
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        bits_contain(&self.bits, key, self.hash_type, self.hash_count)
    }

    /// `BloomFilterChunk.compactBloom`: halve the array while it is far larger than needed.
    ///
    /// Folding is lossless because every probe is `hash % bitSize`: for a power of two fold the
    /// probe over the smaller array lands on the same logical bit.
    pub fn compact(&mut self) {
        if self.key_count == 0 {
            return;
        }
        let mut pieces = 1u64;
        let mut byte_size = self.byte_size;
        let mut max_keys = self.max_keys;
        while byte_size & 1 == 0 && max_keys as i64 > (self.key_count as i64) << 1 {
            pieces <<= 1;
            byte_size >>= 1;
            max_keys >>= 1;
        }
        if pieces == 1 {
            return;
        }
        let new_byte_size = byte_size as usize;
        for i in 0..new_byte_size {
            let mut b = self.bits[i];
            for p in 1..pieces as usize {
                b |= self.bits[i + p * new_byte_size];
            }
            self.bits[i] = b;
        }
        self.bits.truncate(new_byte_size);
        self.byte_size = byte_size;
        self.max_keys = max_keys;
    }

    pub fn byte_size(&self) -> u64 {
        self.byte_size
    }

    pub fn hash_count(&self) -> i32 {
        self.hash_count
    }

    pub fn hash_type(&self) -> i32 {
        self.hash_type
    }

    pub fn key_count(&self) -> i32 {
        self.key_count
    }

    pub fn max_keys(&self) -> i32 {
        self.max_keys
    }

    /// The bit array as written into a `BLMFBLK2` block.
    pub fn bits(&self) -> &[u8] {
        &self.bits
    }
}

fn bits_contain(bits: &[u8], key: &[u8], hash_type: i32, hash_count: i32) -> bool {
    let bit_size = bits.len() as i64 * 8;
    let Ok(probes) = positions(key, hash_type, hash_count, bit_size) else {
        return true; // Unknown hash type: never reject a row (no false negatives).
    };
    for pos in probes {
        let byte = pos >> 3;
        if byte >= bits.len() || bits[byte] & (1 << (pos & 7)) == 0 {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Bloom arithmetic (`BloomFilterUtil`)
// ---------------------------------------------------------------------------

/// `log(2)^2`, the constant HBase uses to size filters.
const LOG2_SQUARED: f64 = std::f64::consts::LN_2 * std::f64::consts::LN_2;

/// `BloomFilterUtil.computeFoldableByteSize`: round the byte size up so it can be halved
/// `max_fold` times.
fn compute_foldable_byte_size(bit_size: i64, max_fold: u32) -> Result<u64> {
    let mut byte_size = (bit_size + 7) / 8;
    let mask = (1i64 << max_fold.min(31)) - 1;
    if mask & byte_size != 0 {
        byte_size >>= max_fold.min(31);
        byte_size += 1;
        byte_size <<= max_fold.min(31);
    }
    if byte_size > i32::MAX as i64 {
        return Err(HFileError::InvalidArgument(format!(
            "bloom filter of {byte_size} bytes is too large"
        )));
    }
    Ok(byte_size as u64)
}

fn ideal_max_keys(bit_size: i64, error_rate: f32) -> i64 {
    (bit_size as f64 * (LOG2_SQUARED / -(error_rate as f64).ln())) as i64
}

fn optimal_function_count(max_keys: i64, bit_size: i64) -> Result<i32> {
    if max_keys <= 0 {
        return Err(HFileError::InvalidArgument(
            "bloom filter size yields zero keys per chunk".into(),
        ));
    }
    let per_key = bit_size / max_keys;
    let count = (std::f64::consts::LN_2 * per_key as f64).ceil();
    Ok(count.max(1.0) as i32)
}

fn compute_max_keys(bit_size: i64, error_rate: f32, hash_count: i32) -> i32 {
    let err = error_rate as f64;
    let value =
        -bit_size as f64 / hash_count as f64 * (1.0 - (err.ln() / hash_count as f64).exp()).ln();
    value as i32
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

/// Where one chunk's block ended up (the metadata's single level index).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomChunkEntry {
    /// First row stored in the chunk.
    pub first_key: Vec<u8>,
    /// Offset of the `BLMFBLK2` block.
    pub offset: u64,
    /// `onDiskSizeWithHeader` of that block.
    pub on_disk_size: u32,
}

struct ReadyChunk {
    first_key: Vec<u8>,
    chunk: BloomChunk,
}

/// `CompoundBloomFilterWriter`: splits rows into fixed size chunks as they arrive.
pub struct BloomFilterWriter {
    options: BloomOptions,
    chunk_byte_size: usize,
    chunk: Option<BloomChunk>,
    first_key_in_chunk: Option<Vec<u8>>,
    ready: std::collections::VecDeque<ReadyChunk>,
    entries: Vec<BloomChunkEntry>,
    hash_count: i32,
    hash_type: i32,
    total_byte_size: i64,
    total_key_count: i64,
    total_max_keys: i64,
    num_chunks: i32,
}

impl BloomFilterWriter {
    pub fn new(options: BloomOptions) -> Result<Self> {
        let chunk_byte_size =
            compute_foldable_byte_size(options.chunk_size as i64 * 8, options.max_fold)?;
        Ok(BloomFilterWriter {
            hash_count: 0,
            hash_type: options.hash_type,
            options,
            chunk_byte_size: chunk_byte_size as usize,
            chunk: None,
            first_key_in_chunk: None,
            ready: std::collections::VecDeque::new(),
            entries: Vec::new(),
            total_byte_size: 0,
            total_key_count: 0,
            total_max_keys: 0,
            num_chunks: 0,
        })
    }

    /// Add one row. Rows must arrive in comparator order, one entry per distinct row.
    pub fn add_row(&mut self, row: &[u8]) -> Result<()> {
        self.enqueue_ready(false)?;
        if self.chunk.is_none() {
            if self.first_key_in_chunk.is_some() {
                return corrupt("bloom chunk already has a first key");
            }
            self.first_key_in_chunk = Some(row.to_vec());
            self.allocate_chunk()?;
        }
        let chunk = self.chunk.as_mut().expect("chunk exists");
        chunk.add(row);
        self.total_key_count += 1;
        Ok(())
    }

    fn allocate_chunk(&mut self) -> Result<()> {
        let chunk = match &self.chunk {
            Some(existing) => existing.create_another(),
            None => BloomChunk::create_by_size(
                self.chunk_byte_size,
                self.options.error_rate,
                self.options.hash_type,
                self.options.max_fold,
            )?,
        };
        self.hash_count = chunk.hash_count();
        self.hash_type = chunk.hash_type();
        self.chunk = Some(chunk);
        self.num_chunks += 1;
        Ok(())
    }

    fn enqueue_ready(&mut self, closing: bool) -> Result<()> {
        let Some(chunk) = &self.chunk else {
            return Ok(());
        };
        if chunk.key_count() < chunk.max_keys() && !closing {
            return Ok(());
        }
        let first_key = self
            .first_key_in_chunk
            .take()
            .ok_or_else(|| HFileError::Corrupt("bloom chunk has no first key".into()))?;
        let mut chunk = self.chunk.take().expect("chunk exists");
        chunk.compact();
        self.total_max_keys += chunk.max_keys() as i64;
        self.total_byte_size += chunk.byte_size() as i64;
        self.ready.push_back(ReadyChunk { first_key, chunk });
        Ok(())
    }

    /// Seal the current chunk and hand out every payload that is ready to be written.
    ///
    /// The caller writes each payload as a `BLMFBLK2` block and reports the location back with
    /// [`BloomFilterWriter::chunk_written`], which keeps the metadata index in write order.
    pub fn take_ready_chunks(&mut self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.enqueue_ready(true)?;
        let mut out = Vec::with_capacity(self.ready.len());
        while let Some(ready) = self.ready.pop_front() {
            out.push((ready.first_key, ready.chunk.bits().to_vec()));
        }
        Ok(out)
    }

    pub fn chunk_written(&mut self, first_key: Vec<u8>, offset: u64, on_disk_size: u32) {
        self.entries.push(BloomChunkEntry {
            first_key,
            offset,
            on_disk_size,
        });
    }

    pub fn num_chunks(&self) -> i32 {
        self.num_chunks
    }

    pub fn key_count(&self) -> i64 {
        self.total_key_count
    }

    /// Payload of the `BLMFMET2` block (`CompoundBloomFilterWriter.MetaWriter`).
    pub fn meta_payload(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(48 + self.entries.len() * 24);
        out.extend_from_slice(&BLOOM_META_VERSION.to_be_bytes());
        out.extend_from_slice(&self.total_byte_size.to_be_bytes());
        out.extend_from_slice(&self.hash_count.to_be_bytes());
        out.extend_from_slice(&self.hash_type.to_be_bytes());
        out.extend_from_slice(&self.total_key_count.to_be_bytes());
        out.extend_from_slice(&self.total_max_keys.to_be_bytes());
        out.extend_from_slice(&self.num_chunks.to_be_bytes());
        // `Bytes.writeByteArray(out, null)`: a zero length vint, because a ROW bloom filter does
        // not need a cell comparator to place its keys.
        write_compressed_int(&mut out, 0);
        // Single level index of the chunks; the entry count is `num_chunks`, not stored here.
        for entry in &self.entries {
            out.extend_from_slice(&(entry.offset as i64).to_be_bytes());
            out.extend_from_slice(&(entry.on_disk_size as i32).to_be_bytes());
            write_compressed_int(&mut out, entry.first_key.len() as i32);
            out.extend_from_slice(&entry.first_key);
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

/// The bloom filter metadata of an open file plus its chunk index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomFilter {
    pub total_byte_size: i64,
    pub hash_count: i32,
    pub hash_type: i32,
    pub total_key_count: i64,
    pub total_max_keys: i64,
    /// Cell comparator the writer recorded; empty for a `ROW` filter.
    pub comparator_class: String,
    pub chunks: Vec<BloomChunkEntry>,
}

impl BloomFilter {
    /// Parse the payload of a `BLMFMET2` block.
    pub fn parse(data: &[u8]) -> Result<Self> {
        let mut cursor = Cursor { data };
        let version = cursor.i32("bloom metadata version")?;
        if version != BLOOM_META_VERSION {
            return unsupported(format!(
                "bloom metadata version {version} is not supported (expected {BLOOM_META_VERSION})"
            ));
        }
        let total_byte_size = cursor.i64("bloom byte size")?;
        let hash_count = cursor.i32("bloom hash count")?;
        let hash_type = cursor.i32("bloom hash type")?;
        let total_key_count = cursor.i64("bloom key count")?;
        let total_max_keys = cursor.i64("bloom max keys")?;
        let num_chunks = cursor.i32("bloom chunk count")?;
        if num_chunks < 0 || hash_count <= 0 {
            return corrupt("bloom metadata has an invalid chunk or hash count");
        }
        if hash_type != MURMUR_HASH {
            return unsupported(format!(
                "bloom hash type {hash_type} is not implemented (only MURMUR_HASH = {MURMUR_HASH})"
            ));
        }
        let comparator = cursor.byte_array("bloom comparator class")?;
        let comparator_class = String::from_utf8_lossy(&comparator).into_owned();

        let mut chunks = Vec::with_capacity(num_chunks as usize);
        for _ in 0..num_chunks {
            let offset = cursor.i64("bloom chunk offset")?;
            let on_disk_size = cursor.i32("bloom chunk size")?;
            let first_key = cursor.byte_array("bloom chunk key")?;
            if offset < 0 || on_disk_size < 0 {
                return corrupt("bloom chunk index has a negative offset or size");
            }
            chunks.push(BloomChunkEntry {
                first_key,
                offset: offset as u64,
                on_disk_size: on_disk_size as u32,
            });
        }
        if !cursor.is_empty() {
            return corrupt("bloom metadata has trailing bytes");
        }
        Ok(BloomFilter {
            total_byte_size,
            hash_count,
            hash_type,
            total_key_count,
            total_max_keys,
            comparator_class,
            chunks,
        })
    }

    /// Index of the chunk that would hold `row`, or `None` when `row` sorts before the file.
    pub fn chunk_for(&self, row: &[u8]) -> Option<usize> {
        let pos = self
            .chunks
            .partition_point(|chunk| chunk.first_key.as_slice() <= row);
        if pos == 0 { None } else { Some(pos - 1) }
    }

    /// Probe one chunk's bit array; `bits` is the payload of its `BLMFBLK2` block.
    pub fn contains_in_chunk(&self, bits: &[u8], row: &[u8]) -> bool {
        bits_contain(bits, row, self.hash_type, self.hash_count)
    }

    /// Bit array size the writer recorded, i.e. the sum over all chunks.
    pub fn byte_size(&self) -> i64 {
        self.total_byte_size
    }
}

struct Cursor<'a> {
    data: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize, what: &str) -> Result<&'a [u8]> {
        if self.data.len() < n {
            return corrupt(format!("bloom metadata is truncated: no {what}"));
        }
        let (head, tail) = self.data.split_at(n);
        self.data = tail;
        Ok(head)
    }

    fn i32(&mut self, what: &str) -> Result<i32> {
        let b = self.take(4, what)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn i64(&mut self, what: &str) -> Result<i64> {
        let b = self.take(8, what)?;
        Ok(i64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn byte_array(&mut self, what: &str) -> Result<Vec<u8>> {
        let (len, used) = read_compressed_int(self.data)?;
        self.data = &self.data[used..];
        if len < 0 {
            return corrupt(format!("{what} has a negative length"));
        }
        Ok(self.take(len as usize, what)?.to_vec())
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn murmur_matches_hbase() {
        // Golden values from HBase 2.5.10 `MurmurHash`, printed by
        // `scripts/hbase/MurmurGolden.java`. Keys 6-18 cover the leftover bytes for every tail
        // length (1, 2 and 3) with and without the high bit, including tails after a full word:
        // those are exactly the cases where a zero extending murmur2 parts ways with HBase.
        const SEEDS: [i32; 4] = [0, 1, 0x5bd1_e995, 7];
        let cases: [(&[u8], [i32; 4]); 18] = [
            (&[], [0, 1540447798, -380735811, -2101582231]),
            (
                b"row000000",
                [-804283873, -1257105191, -1312033284, 2054326625],
            ),
            (b"hello", [-446617141, -1506700914, -1367204360, 88108689]),
            (
                b"0123456789abcdef",
                [-811927782, -794484170, 1107111456, -1951695338],
            ),
            (
                &[0x80, 0x81, 0x82, 0x83, 0x84],
                [-1368132752, 725125259, -951886540, 952012450],
            ),
            // one byte tail
            (&[0x84], [-1533791791, 801409763, -937012344, -358774782]),
            (&[0x04], [9299090, -1268141364, 1274669770, -761340550]),
            // two byte tail
            (
                &[0x00, 0x84],
                [-489032301, 1348170264, -1985193448, -462062461],
            ),
            (
                &[0x84, 0x00],
                [311518853, -780232483, -1685705904, -39096812],
            ),
            (&[0x00, 0x04], [22141236, 1742264827, 480840172, 1709116273]),
            (
                &[0x04, 0x00],
                [-625631775, 733061435, 117572422, -380735811],
            ),
            // three byte tail
            (
                &[0x00, 0x00, 0x84],
                [1837028902, -1678064173, -232988053, -542807564],
            ),
            (
                &[0x84, 0x83, 0x82],
                [-1651901068, -1549838803, 27750145, -938518786],
            ),
            (
                &[0x00, 0x00, 0x04],
                [372180100, -189587647, -25940294, -55759900],
            ),
            // tails after a full word
            (
                &[0x01, 0x02, 0x03, 0x84],
                [-1333669683, 59882035, -1961940081, 1590858967],
            ),
            (
                &[0xff, 0xff, 0xff, 0xff],
                [346862554, 1723358822, -1644330523, -89672208],
            ),
            (
                &[0x01, 0x02, 0x03, 0x04, 0x85, 0x06],
                [742501623, 31548373, 955763168, -30282489],
            ),
            (
                &[0xaa, 0xbb, 0xcc, 0xdd, 0xee],
                [1602768679, 962195801, -612643170, 958413847],
            ),
        ];
        for (data, expected) in cases {
            for (i, seed) in SEEDS.into_iter().enumerate() {
                assert_eq!(
                    murmur_hash(data, seed),
                    expected[i],
                    "murmur_hash({data:02x?}, {seed})"
                );
            }
        }
    }

    #[test]
    fn the_murmur2_crate_parts_ways_on_the_signed_tail() {
        // Documentation with teeth: the `murmur2` crate is stock `MurmurHash2`, so it may be used
        // for the body and the finalisation but never for the leftover bytes, which HBase reads as
        // signed Java bytes. These assertions are what `HFileReader`/`HFileWriter` rely on.
        let agrees: [&[u8]; 7] = [
            &[],
            b"0123456789abcdef",
            b"hello",
            &[0x04],
            &[0x00, 0x04],
            &[0x00, 0x00, 0x04],
            &[0x00, 0x00, 0x00, 0x04],
        ];
        for key in agrees {
            assert_eq!(
                murmur2::murmur2(key, 0) as i32,
                murmur_hash(key, 0),
                "crate and HBase must agree on {key:02x?}"
            );
        }

        let diverges: [&[u8]; 4] = [
            &[0x84],
            &[0x00, 0x84],
            &[0x84, 0x83, 0x82],
            &[0x80, 0x81, 0x82, 0x83, 0x84],
        ];
        for key in diverges {
            assert_ne!(
                murmur2::murmur2(key, 0) as i32,
                murmur_hash(key, 0),
                "the crate zero extends the tail of {key:02x?}"
            );
        }
        assert_eq!(murmur_hash(&[0x80, 0x81, 0x82, 0x83, 0x84], 0), -1368132752);
        assert_eq!(
            murmur2::murmur2(&[0x80, 0x81, 0x82, 0x83, 0x84], 0) as i32,
            -1237960112
        );
    }

    #[test]
    fn chunk_parameters_match_hbase_defaults() {
        let chunk = BloomChunk::create_by_size(
            DEFAULT_CHUNK_SIZE,
            DEFAULT_ERROR_RATE,
            MURMUR_HASH,
            DEFAULT_MAX_FOLD,
        )
        .unwrap();
        // 128 KiB of bits, 7 hash functions, ~109k keys at a 1% error rate.
        assert_eq!(chunk.byte_size(), 128 * 1024);
        assert_eq!(chunk.hash_count(), 7);
        assert!(
            (109_300..109_500).contains(&chunk.max_keys()),
            "{}",
            chunk.max_keys()
        );
    }

    #[test]
    fn no_false_negatives_and_few_false_positives() {
        let mut chunk =
            BloomChunk::create_by_size(4096, DEFAULT_ERROR_RATE, MURMUR_HASH, DEFAULT_MAX_FOLD)
                .unwrap();
        let present: Vec<Vec<u8>> = (0..chunk.max_keys())
            .map(|i| format!("row{i:06}").into_bytes())
            .collect();
        for row in &present {
            chunk.add(row);
        }
        for row in &present {
            assert!(chunk.contains(row), "false negative for {:?}", row);
        }
        let absent: Vec<Vec<u8>> = (0..10_000)
            .map(|i| format!("zzz{i:06}").into_bytes())
            .collect();
        let positives = absent.iter().filter(|r| chunk.contains(r)).count();
        // 1% target, allow generous slack for a small filter.
        assert!(positives < 500, "{positives} false positives");
    }

    #[test]
    fn folding_keeps_membership() {
        let mut small = BloomChunk::create_by_size(
            64 * 1024,
            DEFAULT_ERROR_RATE,
            MURMUR_HASH,
            DEFAULT_MAX_FOLD,
        )
        .unwrap();
        let rows: Vec<Vec<u8>> = (0..50).map(|i| format!("row{i}").into_bytes()).collect();
        for row in &rows {
            small.add(row);
        }
        let before = small.byte_size();
        small.compact();
        assert!(small.byte_size() < before, "chunk was not folded");
        for row in &rows {
            assert!(small.contains(row), "folding lost {:?}", row);
        }
    }

    #[test]
    fn metadata_roundtrip() {
        let mut writer = BloomFilterWriter::new(BloomOptions::default()).unwrap();
        for i in 0..200_000 {
            writer.add_row(format!("row{i:08}").as_bytes()).unwrap();
        }
        let ready = writer.take_ready_chunks().unwrap();
        assert_eq!(ready.len(), 2, "200k rows need two chunks");
        let mut offset = 1000u64;
        for (first_key, bits) in ready {
            let size = bits.len() as u32 + 33;
            writer.chunk_written(first_key, offset, size);
            offset += size as u64;
        }
        let payload = writer.meta_payload();
        let parsed = BloomFilter::parse(&payload).unwrap();
        assert_eq!(parsed.chunks.len(), 2);
        assert_eq!(parsed.hash_count, 7);
        assert_eq!(parsed.comparator_class, "");
        assert_eq!(parsed.total_key_count, 200_000);
        // Every row maps into a chunk, and the first chunk starts at the first row.
        assert_eq!(parsed.chunk_for(b"row00000000"), Some(0));
        assert_eq!(parsed.chunk_for(b"row00199999"), Some(1));
        assert_eq!(parsed.chunk_for(b"aaa"), None);
    }
}
