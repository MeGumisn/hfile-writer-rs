//! Compile `proto/hfile.proto` with `protox` so the build works without a `protoc` binary.

fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=proto/hfile.proto");
    let fds = protox::compile(["proto/hfile.proto"], ["proto/"])
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    prost_build::Config::new()
        .compile_fds(fds)
        .map_err(std::io::Error::other)
}
