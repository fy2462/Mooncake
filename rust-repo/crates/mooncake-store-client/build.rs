use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_path = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?)
        .ancestors()
        .nth(2)
        .unwrap()
        .join("proto");

    // Master service proto — client stubs only.
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
            std::slice::from_ref(&proto_path),
        )?;

    // Offload RPC proto — both client and server.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[proto_path
                .join("mooncake_offload_rpc.proto")
                .to_str()
                .unwrap()],
            std::slice::from_ref(&proto_path),
        )?;

    Ok(())
}
