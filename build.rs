fn main() {
    // trigger recompilation when a new migration is added
    println!("cargo:rerun-if-changed=src/storage/postgresql/migrations");
    println!("cargo:rerun-if-changed=src/storage/sqlite/migrations");
}
