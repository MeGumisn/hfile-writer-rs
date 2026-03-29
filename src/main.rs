fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    #[test]
    fn it_works() {
        // HFILEBLOCK_DUMMY_HEADER占位
        let mut baosInMemory = Cursor::new(vec![0u8; 33]);
        baosInMemory.set_position(33);
        let mut kv_writer = Cursor::new(vec![0u8; 32]);
        let row_key = "test_row_00".as_bytes();
        let column_family = "test_cf_00".as_bytes();
        let qualifier = "test_qualifier_00".as_bytes();
        let value = "test_value_00".as_bytes();

        let key_length = 12 + row_key.len() + column_family.len() + qualifier.len();
        let kv_length = 8 + key_length + value.len();

        kv_writer
            .write_all((key_length as i32).to_be_bytes().as_ref())
            .unwrap();
        kv_writer
            .write_all((value.len() as i32).to_be_bytes().as_ref())
            .unwrap();
        kv_writer
            .write_all(((row_key.len() & 0xffff) as i16).to_be_bytes().as_ref())
            .unwrap();
        kv_writer.write_all(row_key).unwrap();
        if !column_family.is_empty() {
            kv_writer
                .write_all((column_family.len() & 0xffff).to_be_bytes().as_ref())
                .unwrap();
            kv_writer.write_all(column_family).unwrap();
        }
        if !qualifier.is_empty() {
            kv_writer
                .write_all((qualifier.len() & 0xffff).to_be_bytes().as_ref())
                .unwrap();
            kv_writer.write_all(qualifier).unwrap();
        }
        // timestamp
        kv_writer
            .write_all(i64::MAX.to_be_bytes().as_ref())
            .unwrap();
        // type PUT code
        kv_writer.write(&[4u8]).unwrap();
        // value
        kv_writer.write(value).unwrap();
        println!("{:#?}", kv_writer);
    }
}
