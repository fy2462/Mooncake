use std::env;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    // The crate is at rust-repo/crates/transfer-engine-ffi; go up 3 levels to the
    // Mooncake repo root where mooncake-transfer-engine/include/ lives.
    let project_root = manifest_dir.ancestors().nth(3).unwrap();

    let te_include = project_root
        .join("mooncake-transfer-engine")
        .join("include");
    let te_c_header = te_include.join("transfer_engine_c.h");
    let accelerator_c_header = te_include.join("accelerator_memory_c.h");
    let tent_include = project_root
        .join("mooncake-transfer-engine")
        .join("tent")
        .join("include");
    let tent_c_header = tent_include.join("tent").join("transfer_engine.h");

    if !te_c_header.exists() {
        println!(
            "cargo:warning=Transfer Engine C header not found at {}; FFI bindings will be generated from stub declarations.",
            te_c_header.display()
        );
    }

    let bindings = bindgen::Builder::default()
        .header(
            te_c_header
                .to_str()
                .unwrap_or("mooncake-transfer-engine/include/transfer_engine_c.h"),
        )
        .header(
            accelerator_c_header
                .to_str()
                .unwrap_or("mooncake-transfer-engine/include/accelerator_memory_c.h"),
        )
        .header(
            tent_c_header
                .to_str()
                .unwrap_or("mooncake-transfer-engine/tent/include/tent/transfer_engine.h"),
        )
        .clang_args(&[
            "-x",
            "c",
            &format!("-I{}", te_include.display()),
            &format!("-I{}", tent_include.display()),
        ])
        .allowlist_type("transfer_engine_t")
        .allowlist_type("transport_t")
        .allowlist_type("transfer_request_t")
        .allowlist_type("tent_engine_t")
        .allowlist_type("tent_batch_id_t")
        .allowlist_type("tent_segment_id_t")
        .allowlist_type("tent_request_t")
        .allowlist_type("tent_request_v2_t")
        .allowlist_type("tent_status_t")
        .allowlist_type("tent_metrics_status_v1_t")
        .allowlist_type("transfer_status_t")
        .allowlist_type("segment_desc_t")
        .allowlist_type("buffer_entry_t")
        .allowlist_type("notify_msg_t")
        .allowlist_type("nic_load_stat_t")
        .allowlist_type("tent_nic_load_stat_t")
        .allowlist_var("OPCODE_READ")
        .allowlist_var("OPCODE_WRITE")
        .allowlist_var("STATUS_.*")
        .allowlist_var("LOCAL_SEGMENT")
        .allowlist_var("INVALID_BATCH")
        .allowlist_var("TENT_REQUEST_V2_VERSION")
        .allowlist_var("TENT_INTENT_.*")
        .allowlist_var("TRANSPORT_.*")
        .allowlist_var("PERM_.*")
        .allowlist_var("MEMORY_POINTER_.*")
        .allowlist_function("classifyMemoryPointer")
        .allowlist_function("copyMemoryToHost")
        .allowlist_function("copyMemoryFromHost")
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
        .allowlist_function("getRpcPort")
        .allowlist_function("registerLocalMemory")
        .allowlist_function("unregisterLocalMemory")
        .allowlist_function("registerLocalMemoryBatch")
        .allowlist_function("unregisterLocalMemoryBatch")
        .allowlist_function("allocateBatchID")
        .allowlist_function("submitTransfer")
        .allowlist_function("tent_.*")
        .allowlist_function("submitTransferWithNotify")
        .allowlist_function("getTransferStatus")
        .allowlist_function("getBatchTransferStatus")
        .allowlist_function("freeBatchID")
        .allowlist_function("getSegmentBufferAddr")
        .allowlist_function("syncSegmentCache")
        .allowlist_function("checkSegmentStatus")
        .allowlist_function("genNotifyInEngine")
        .allowlist_function("getNotifsFromEngine")
        .allowlist_function("freeNotifsMsgBuf")
        .allowlist_function("probePeerAliveByID")
        .allowlist_function("isTcpOnly")
        .allowlist_function("checkOverlap")
        .allowlist_function("setAutoDiscover")
        .allowlist_function("getBaseAddr")
        .allowlist_function("getNicLoadStats")
        .generate_comments(false)
        .layout_tests(false)
        .generate()
        .expect("Failed to generate Transfer Engine bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("transfer_engine_bindings.rs");
    // bindgen 0.70 predates Edition 2024's requirement that extern blocks are
    // explicitly unsafe. Keep the generated declarations compatible without
    // requiring an unrelated dependency upgrade.
    let bindings = bindings
        .to_string()
        .replace("extern \"C\" {", "unsafe extern \"C\" {");
    std::fs::write(out_path, bindings).expect("Failed to write Transfer Engine bindings");

    // Link against the Transfer Engine shared library only when the
    // "link-native" feature is enabled.  Tests and type-level code
    // compile without the native library.
    let link_native = std::env::var("CARGO_FEATURE_LINK_NATIVE").is_ok();
    if link_native {
        println!("cargo:rustc-link-lib=dylib=transfer_engine");

        // Add common search paths for the Mooncake build output.
        let build_dir = project_root.join("build");
        if build_dir
            .join("mooncake-transfer-engine")
            .join("src")
            .exists()
        {
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
            build_dir.join("mooncake-common").join("src").display()
        );

        if std::env::var("CARGO_FEATURE_LINK_TENT_NATIVE").is_ok() {
            println!("cargo:rustc-link-lib=dylib=tent_shared");
            println!(
                "cargo:rustc-link-search=native={}",
                build_dir
                    .join("mooncake-transfer-engine")
                    .join("tent")
                    .join("src")
                    .display()
            );
        }
    }
}
