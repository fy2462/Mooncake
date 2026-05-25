use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_path = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?)
        .ancestors()
        .nth(2)
        .unwrap()
        .join("proto");

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(
            &[
                proto_path
                    .join("mooncake_store_grpc.proto")
                    .to_str()
                    .unwrap(),
                proto_path
                    .join("mooncake_store_types.proto")
                    .to_str()
                    .unwrap(),
            ],
            &[proto_path],
        )?;

    Ok(())
}
