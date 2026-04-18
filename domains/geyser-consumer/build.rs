fn main() {
    tonic_build::configure()
        .compile(
            &["proto/geyser.proto", "proto/solana-storage.proto"],
            &["proto"],
        )
        .expect("Failed to compile protos");
}
