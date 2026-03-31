pub mod hbase;

const ROW_LENGTH_SIZE: i32 = 2;
const FAMILY_LENGTH_SIZE: i32 = 1;

const TIMESTAMP_SIZE: i32 = 8;
const TYPE_SIZE: i32 = 1;
const TIMESTAMP_TYPE_SIZE: i32 = TIMESTAMP_SIZE + TYPE_SIZE;
const KEY_INFRASTRUCTURE_SIZE: i32 = ROW_LENGTH_SIZE + FAMILY_LENGTH_SIZE + TIMESTAMP_TYPE_SIZE;
/**
 * Computes the number of bytes that a <code>KeyValue</code> instance with the provided
 * characteristics would take up in its underlying data structure for the key.
 * @param rlength row length
 * @param flength family length
 * @param qlength qualifier length
 * @return the key data structure length
 */
#[inline]
fn get_key_data_structure_size(row_length: i32, family_length: i32, qualifier_length: i32) -> i32 {
    KEY_INFRASTRUCTURE_SIZE + row_length + family_length + qualifier_length
}

const ROW_OFFSET: i32 = (size_of::<i32>() + size_of::<i32>()) as i32;
const KEY_VALUE_INFRASTRUCTURE_SIZE: i32 = ROW_OFFSET;
const TAGS_LENGTH_SIZE: i32 = size_of::<i16>() as i32;
const KEY_VALUE_WITH_TAGS_INFRASTRUCTURE_SIZE: i32 = ROW_OFFSET + TAGS_LENGTH_SIZE;
#[inline]
fn get_key_value_data_structure_size(
    row_length: i32,
    family_length: i32,
    qualifier_length: i32,
    value_length: i32,
    tags_length: i32,
) -> i32 {
    if tags_length == 0 {
        KEY_VALUE_INFRASTRUCTURE_SIZE
            + get_key_data_structure_size(row_length, family_length, qualifier_length)
    } else {
        KEY_VALUE_WITH_TAGS_INFRASTRUCTURE_SIZE
            + get_key_data_structure_size(row_length, family_length, qualifier_length)
            + value_length
            + tags_length
    }
}

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    #[test]
    fn it_works() {
        // HFILEBLOCK_DUMMY_HEADER占位
        let mut data_stream = Cursor::new(vec![0u8; 33]);
        data_stream.set_position(33);
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
