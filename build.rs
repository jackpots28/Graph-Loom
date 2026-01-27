fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "api")]
    tonic_build::compile_protos("proto/graph_loom.proto")?;
    Ok(())
}
