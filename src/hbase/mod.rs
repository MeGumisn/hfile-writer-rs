use bytes::TryGetError;
use std::io::{Cursor, Error};
use tokio::io::AsyncWriteExt;

pub struct KeyValue {
    // row_key, cf, qualifier, value， timestamp 都存里面, 只记录offset
    row: Cursor<Vec<u8>>,
    row_offset: u64,
    row_length: usize,
    family_offset: usize,
    family_length: usize,
    qualifier_offset: usize,
    qualifier_length: usize,
    timestamp: u64,
    r#type: u8,
    value_offset: usize,
    value_length: usize,
    seq_id: u64,
    tags_offset: usize,
    tags_length: usize,
}

impl KeyValue {
    pub async fn new(&mut self, row_key: &[u8], column_family:&[u8], qualifier:&[u8], value:&[u8]) -> Result<(), Error> {
        self.row_offset = 0;
        self.row_length = row_key.len();
        self.row.set_position(self.row_offset);
        Ok(self.row.write_all(row_key).await?)
    }
}
