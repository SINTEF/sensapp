fn main() {
    // trigger recompilation when a new migration is added
    println!("cargo:rerun-if-changed=src/storage/postgresql/migrations");
    println!("cargo:rerun-if-changed=src/storage/sqlite/migrations");

    // On macOS, the downloaded `libduckdb.dylib` sits in `target/<profile>/deps`. Cargo tells the
    // test binaries where it is, but not the doctests, which rustdoc builds and runs from a
    // temporary directory: they could not load it. An rpath to that directory fixes them.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos")
        && std::env::var_os("CARGO_FEATURE_DUCKDB").is_some()
        && let Some(out_dir) = std::env::var_os("OUT_DIR")
    {
        // OUT_DIR is target/<profile>/build/sensapp-<hash>/out
        let deps = std::path::Path::new(&out_dir).join("../../../deps");
        if let Ok(deps) = deps.canonicalize() {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", deps.display());
        }
    }
}
