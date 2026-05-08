use crate::hbase::hfile_pb::FileTrailerProto;
use bstr::ByteSlice;
use integer_encoding::VarIntReader;
use prost::Message;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

fn read_hfile_version(file: &mut File) -> Result<(i32, i32), std::io::Error> {
    let buf = &mut [0u8; 4];
    file.seek(SeekFrom::End(0 - size_of::<i32>() as i64))?;
    file.read(buf)?;
    let materialized_version = i32::from_be_bytes(*buf);
    let minor = materialized_version >> 24;
    let major = materialized_version & 0x00FF_FFFF;
    Ok((major, minor))
}

fn read_hfile_trailer(file: &mut File) -> Result<FileTrailerProto, std::io::Error> {
    file.seek(SeekFrom::End((0 - 4096) as i64))?;
    let trailer_magic = &mut [0u8; 8];
    file.read(trailer_magic)?;
    println!("{:?}", trailer_magic.to_str());
    let var_int_pb_length = file.read_varint::<u32>()?;
    let mut trailer = vec![0u8; var_int_pb_length as usize];
    file.read(&mut trailer)?;
    let hfile_trailer = FileTrailerProto::decode(trailer.as_slice())?;
    Ok(hfile_trailer)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_read_hfile_version() {
        let (major, minor) = read_hfile_version(&mut File::open("test_file").unwrap()).unwrap();
        println!("major version: {}", major);
        println!("minor version: {}", minor);
    }

    #[test]
    fn test_read_hfile_trailer() {
        let trailer = read_hfile_trailer(&mut File::open("test_file").unwrap()).unwrap();
        println!("trailer: {:#?}", trailer);
    }
}
