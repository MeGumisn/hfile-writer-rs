fn main() -> std::io::Result<()> {
    // 告诉 prost 你的 proto 文件在哪里，以及它所在的文件夹（包含路径）
    prost_build::compile_protos(&["src/hfile_trailer.proto"], &["src/"])?;
    Ok(())
}