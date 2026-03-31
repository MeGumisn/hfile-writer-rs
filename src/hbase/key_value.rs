use bytes::Bytes;
use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Error};
use bstr::ByteSlice;
use tokio::io::AsyncWriteExt;
use utctimestamp::UtcTimeStamp;

const ROW_LENGTH_SIZE: usize = size_of::<u16>();
const FAMILY_LENGTH_SIZE: usize = size_of::<u8>();
const TIMESTAMP_SIZE: usize = size_of::<u64>();
const TYPE_SIZE: usize = size_of::<u8>();
const KEY_INFRASTRUCTURE_SIZE: usize =
    ROW_LENGTH_SIZE + FAMILY_LENGTH_SIZE + TIMESTAMP_SIZE + TYPE_SIZE;

/// key_len + value_len
const ROW_OFFSET: usize = size_of::<u32>() + size_of::<u32>();
const KEY_VALUE_INFRASTRUCTURE_SIZE: usize = ROW_OFFSET;
const TAGS_LENGTH_SIZE: usize = size_of::<u16>();
const KEY_VALUE_WITH_TAGS_INFRASTRUCTURE_SIZE: usize = ROW_OFFSET + TAGS_LENGTH_SIZE;

pub enum CellType {}
impl CellType {
    const PUT: u8 = 4;
}
pub struct KeyValue {
    // row_key, cf, qualifier, value， timestamp 都存里面, 只记录offset
    row: Cursor<Vec<u8>>,
    row_offset: u64,
    row_length: usize,
    family_offset: u64,
    family_length: usize,
    qualifier_offset: u64,
    qualifier_length: usize,
    timestamp: i64,
    r#type: u8,
    value_offset: u64,
    value_length: usize,
    seq_id: u64,
    tags_offset: u64,
    tags_length: usize,
}

impl KeyValue {
    #[inline]
    pub fn get_key_data_structure_size(
        row_len: usize,
        family_len: usize,
        qualifier_len: usize,
    ) -> usize {
        KEY_INFRASTRUCTURE_SIZE + row_len + family_len + qualifier_len
    }

    #[inline]
    pub fn get_key_value_data_structure_size(
        row_len: usize,
        family_len: usize,
        qualifier_len: usize,
        value_len: usize,
        tags_len: usize,
    ) -> usize {
        if tags_len == 0 {
            KEY_VALUE_INFRASTRUCTURE_SIZE
                + Self::get_key_data_structure_size(row_len, family_len, qualifier_len)
                + value_len
        } else {
            KEY_VALUE_WITH_TAGS_INFRASTRUCTURE_SIZE
                + Self::get_key_data_structure_size(row_len, family_len, qualifier_len)
                + value_len
                + tags_len
        }
    }
    pub async fn new(
        row_key: &[u8],
        column_family: &[u8],
        qualifier: &[u8],
        value: Option<&[u8]>,
        tags: Option<&[u8]>,
    ) -> Result<Self, Error> {
        let row_length = row_key.len();
        let family_length = column_family.len();
        let qualifier_length = qualifier.len();
        let (value, value_length) = if let Some(value) = value {
            (value, value.len())
        } else {
            ([].as_slice(), 0)
        };
        let (tags, tags_length) = if let Some(tags) = tags {
            (tags, tags.len())
        } else {
            ([].as_slice(), 0)
        };
        let kv_size = Self::get_key_value_data_structure_size(
            row_length,
            family_length,
            qualifier_length,
            value_length,
            tags_length,
        );
        let byte_array = vec![0u8; kv_size];
        let mut cursor = Cursor::new(byte_array);

        let key_len =
            Self::get_key_data_structure_size(row_length, family_length, qualifier_length);
        cursor.write_i32(key_len as i32).await?;
        cursor.write_i32(value_length as i32).await?;
        cursor.write_u16(row_length as u16).await?;

        let row_offset = cursor.position();
        AsyncWriteExt::write_all(&mut cursor, row_key).await?;
        cursor.write_u8(family_length as u8).await?;

        let family_offset = cursor.position();
        if family_length > 0 {
            AsyncWriteExt::write_all(&mut cursor, column_family).await?;
        }

        let qualifier_offset = cursor.position();
        if qualifier_length > 0 {
            AsyncWriteExt::write_all(&mut cursor, qualifier).await?;
        }

        let timestamp = UtcTimeStamp::now().as_milliseconds();
        cursor.write_i64(timestamp).await?;
        let r#type = CellType::PUT;
        cursor.write_u8(CellType::PUT).await?;

        let value_offset = cursor.position();
        if value_length > 0 {
            AsyncWriteExt::write_all(&mut cursor, value).await?;
        }

        let tags_offset;
        if tags_length > 0 {
            cursor.write_u16(tags_length as u16).await?;
            tags_offset = cursor.position();
            AsyncWriteExt::write_all(&mut cursor, tags).await?;
        } else {
            tags_offset = cursor.position();
        }

        Ok(Self {
            row: cursor,
            row_offset,
            row_length,
            family_offset,
            family_length,
            qualifier_offset,
            qualifier_length,
            timestamp,
            r#type,
            value_offset,
            value_length,
            seq_id: 0,
            tags_offset,
            tags_length,
        })
    }

    pub fn get_seq_id(&self) -> u64 {
        self.seq_id
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.row.into_inner()
    }
}

impl Debug for KeyValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let inner = self.row.get_ref();
        let row = Bytes::copy_from_slice(
            &inner[self.row_offset as usize..self.row_offset as usize + self.row_length],
        );
        let family = Bytes::copy_from_slice(
            &inner[self.family_offset as usize..self.family_offset as usize + self.family_length],
        );
        let qualifier = Bytes::copy_from_slice(
            &inner[self.qualifier_offset as usize
                ..self.qualifier_offset as usize + self.qualifier_length],
        );
        let value = Bytes::copy_from_slice(
            &inner[self.value_offset as usize..self.value_offset as usize + self.value_length],
        );
        let tags = Bytes::copy_from_slice(
            &inner[self.tags_offset as usize..self.tags_offset as usize + self.tags_length],
        );
        f.debug_struct("KeyValue")
            .field("row", &row.as_bstr())
            .field("family", &family.as_bstr())
            .field("qualifier", &qualifier.as_bstr())
            .field("timestamp", &self.timestamp)
            .field("r#type", &self.r#type)
            .field("value", &value.as_bstr())
            .field("tags", &tags.as_bstr())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_key_value()->Result<(),Error>{
        let row_key = "test_row_00".as_bytes();
        let family = "test_family_00".as_bytes();
        let qualifier = "test_qualifier_00".as_bytes();
        let value = "test_value_00".as_bytes();

        let kv = KeyValue::new(row_key,family,qualifier,Some(value),None).await?;
        println!("{:#?}", kv);
        Ok(())
    }
}