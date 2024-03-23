## Object Store Rust/Go comparison

This tests StreamingFast `dstore` Golang library speed against Rust `object-store` library when reading StreamingFast merged blocks.

The pass `store_url` is expected to contains a blocks for a network, the CLI application in both language will read blocks for 2 minutes and output average speed each 5s as well as final average download speed over 2 minutes.

### Golang

```bash
cd go
go run . gs://<bucket>/<path> <block_offset>
```

### Rust

```bash
cargo run -- gs://<bucket>/<path> <block_offset>
```
