use std::env;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    // The crate is at rust-repo/crates/transfer-engine-ffi; go up 3 levels to the
    // Mooncake repo root where mooncake-transfer-engine/include/ lives.
    let project_root = manifest_dir.ancestors().nth(3).unwrap();

    let te_include = project_root.join("mooncake-transfer-engine").join("include");
    let te_c_header = te_include.join("transfer_engine_c.h");

    if !te_c_header.exists() {
        println!("cargo:warning=Transfer Engine C header not found at {}; FFI bindings will be generated from stub declarations.", te_c_header.display());
    }

    let bindings = bindgen::Builder::default()
        .header(
            te_c_header
                .to_str()
                .unwrap_or("mooncake-transfer-engine/include/transfer_engine_c.h"),
        )
        .clang_args(&[
            "-x", "c",
            &format!("-I{}", te_include.display()),
        ])
        .allowlist_type("transfer_engine_t")
        .allowlist_type("transport_t")
        .allowlist_type("transfer_request_t")
        .allowlist_type("transfer_status_t")
        .allowlist_type("segment_desc_t")
        .allowlist_type("buffer_entry_t")
        .allowlist_type("notify_msg_t")
        .allowlist_var("OPCODE_READ")
        .allowlist_var("OPCODE_WRITE")
        .allowlist_var("STATUS_.*")
        .allowlist_var("LOCAL_SEGMENT")
        .allowlist_var("INVALID_BATCH")
        .allowlist_function("createTransferEngine")
        .allowlist_function("destroyTransferEngine")
        .allowlist_function("installTransport")
        .allowlist_function("uninstallTransport")
        .allowlist_function("openSegment")
        .allowlist_function("openSegmentNoCache")
        .allowlist_function("closeSegment")
        .allowlist_function("warmupEfaSegment")
        .allowlist_function("removeLocalSegment")
        .allowlist_function("discoverTopology")
        .allowlist_function("getLocalIpAndPort")
        .allowlist_function("registerLocalMemory")
        .allowlist_function("unregisterLocalMemory")
        .allowlist_function("registerLocalMemoryBatch")
        .allowlist_function("unregisterLocalMemoryBatch")
        .allowlist_function("allocateBatchID")
        .allowlist_function("submitTransfer")
        .allowlist_function("submitTransferWithNotify")
        .allowlist_function("getTransferStatus")
        .allowlist_function("freeBatchID")
        .allowlist_function("syncSegmentCache")
        .allowlist_function("genNotifyInEngine")
        .allowlist_function("getNotifsFromEngine")
        .allowlist_function("freeNotifsMsgBuf")
        .generate_comments(false)
        .layout_tests(false)
        .generate()
        .expect("Failed to generate Transfer Engine bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("transfer_engine_bindings.rs");
    bindings
        .write_to_file(out_path)
        .expect("Failed to write Transfer Engine bindings");

    // Link against the Transfer Engine shared library.
    // In practice you need libtransfer_engine.so on the linker search path.
    println!("cargo:rustc-link-lib=dylib=transfer_engine");

    // Add common search paths for the Mooncake build output.
    let build_dir = project_root.join("build");
    if build_dir.join("mooncake-transfer-engine").join("src").exists() {
        println!(
            "cargo:rustc-link-search=native={}",
            build_dir
                .join("mooncake-transfer-engine")
                .join("src")
                .display()
        );
    }
    println!(
        "cargo:rustc-link-search=native={}",
        build_dir
            .join("mooncake-common")
            .join("src")
            .display()
    );
}
