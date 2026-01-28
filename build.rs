fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "api")]
    tonic_build::compile_protos("proto/graph_loom.proto")?;

    #[cfg(target_os = "windows")]
    {
        embed_resource::compile("app.rc", embed_resource::NONE);
    }

    Ok(())
}
