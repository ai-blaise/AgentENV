fn main() {
    let proto = "../../services/api/proto/scheduler.proto";
    println!("cargo:rerun-if-changed={proto}");
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&[proto], &["../../services/api/proto"])
        .expect("failed to compile scheduler protocol");
}
