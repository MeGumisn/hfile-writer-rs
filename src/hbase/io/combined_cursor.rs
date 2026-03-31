use std::hash::Hasher;
use std::io;
use std::io::{Cursor, Error, Read, Seek, SeekFrom, Write};
use tokio::io::AsyncWriteExt;
use crate::hbase::key_value::KeyValue;

pub enum ChecksumType {}
impl ChecksumType {
    const CRC32C: u8 = 2;
}

pub enum BlockType {}
impl BlockType {
    const DATA: &[u8] = "DATABLK*".as_bytes();
}

#[derive(Debug)]
pub struct ChainedCursor {
    header: Vec<u8>,
    block_data: Vec<u8>,
    crc_data: Vec<u8>,
    pos: usize, // 新增：追蹤讀取位置
}

impl ChainedCursor {
    async fn put_header(
        cursor: &mut Cursor<Vec<u8>>,
        on_disk_block_data_size: usize,
        uncompressed_size: usize,
        on_disk_block_size_without_header: usize,
    ) -> Result<(), Error> {
        let pos = cursor.position();
        cursor.set_position(0);
        AsyncWriteExt::write_all(cursor, BlockType::DATA).await?;
        cursor.write_i32(on_disk_block_data_size as i32).await?;
        cursor.write_i32(uncompressed_size as i32).await?;
        // TODO prev_offset, 这里只写了一个block, 所以暂定-1
        cursor.write_i64(-1).await?;
        cursor.write_u8(ChecksumType::CRC32C).await?;
        // TODO bytes_per_checksum=16384
        cursor.write_i32(16384).await?;
        cursor
            .write_i32(on_disk_block_size_without_header as i32 + 33)
            .await?;
        cursor.set_position(pos);
        Ok(())
    }
    pub async fn new(key_value: KeyValue, is_includes_mvcc: bool) -> Result<Self, Error> {
        let seq_id = key_value.get_seq_id();
        let mut block_data = key_value.into_inner();
        if is_includes_mvcc {
            let mut block_cursor = Cursor::new(block_data);
            block_cursor.seek(SeekFrom::End(0))?;
            Self::write_vlong(&mut block_cursor, seq_id as i64)?;
            block_data = block_cursor.into_inner();
        }
        let crc_size = (1 + (block_data.len() + 33) / 16384) * 4;
        let on_disk_size_without_header = block_data.len();
        let on_disk_data_size_without_header = block_data.len() + crc_size;
        let uncompressed_data_size = on_disk_size_without_header;
        let dummy_header = vec![0u8; 33];
        let mut cursor = Cursor::new(dummy_header);
        Self::put_header(
            &mut cursor,
            on_disk_data_size_without_header,
            uncompressed_data_size,
            on_disk_size_without_header,
        ).await?;

        let header = cursor.into_inner();

        // TODO 这里只写了一个block, 所以这边只计算了一次
        let mut hasher = crc32c::Crc32cHasher::new(0);
        let _ = hasher.write(&header);
        let _ = hasher.write(&block_data);
        let crc_data = (hasher.finish() as i32).to_le_bytes().to_vec();

        // 这里计算header内容
        Ok(Self {
            header,
            block_data,
            crc_data,
            pos: 0,
        })
    }

    pub fn reset(&mut self) {
        self.pos = 0;
    }

    pub fn write_vlong<W: Write>(stream: &mut W, mut i: i64) -> io::Result<()> {
        // 情況 1: 單位元組直接編碼 [-112, 127]
        if i >= -112 && i <= 127 {
            stream.write_all(&[i as i8 as u8])?;
            return Ok(());
        }

        let mut len = -112i8;
        if i < 0 {
            i ^= -1i64; // 取反碼 (one's complement)
            len = -120;
        }

        // 計算需要多少個位元組來儲存
        let mut tmp = i;
        while tmp != 0 {
            tmp >>= 8;
            len -= 1;
        }

        // 寫入長度標記位元組
        stream.write_all(&[len as u8])?;

        // 計算實際數據位元組數
        // Java: len = (len < -120) ? -(len + 120) : -(len + 112);
        let actual_len = if len < -120 {
            -(len + 120)
        } else {
            -(len + 112)
        } as usize;

        // 從高位到低位寫入數據位元組
        for idx in (1..=actual_len).rev() {
            let shiftbits = (idx - 1) * 8;
            let mask = 0xFFi64 << shiftbits;
            let byte_val = ((i & mask) >> shiftbits) as u8;
            stream.write_all(&[byte_val])?;
        }

        Ok(())
    }
}

impl Read for ChainedCursor {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut bytes_written = 0;
        while bytes_written < buf.len() {
            let current_chunk = if self.pos < self.header.len() {
                &self.header[self.pos..]
            } else if self.pos < self.header.len() + self.block_data.len() {
                &self.block_data[self.pos - self.header.len()..]
            } else if self.pos < self.header.len() + self.block_data.len() + self.crc_data.len() {
                &self.crc_data[self.pos - self.header.len() - self.block_data.len()..]
            } else {
                break; // 已讀完所有數據
            };

            let n = (&mut buf[bytes_written..]).write(current_chunk)?;
            if n == 0 {
                break;
            }

            self.pos += n;
            bytes_written += n;
        }

        Ok(bytes_written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_combined_cursor()->Result<(), Error> {
        let row_key = "test_row_00".as_bytes();
        let family = "test_family_00".as_bytes();
        let qualifier = "test_qualifier_00".as_bytes();
        let value = "test_value_00".as_bytes();
        let kv = KeyValue::new(row_key,family,qualifier,Some(value),None).await?;

        let chained_cursor = ChainedCursor::new(kv, true).await?;
        println!("{:?}",chained_cursor);
        Ok(())
    }
}
