# rustcoon

A DICOM PACS built in Rust.

> Work in progress. APIs, crate boundaries, and runtime configuration may change.

## Getting Started

### Run Locally

```
cargo run -p rustcoon
```

## Configuration

The monolith binary runs with built-in defaults. Optional overrides can be loaded from `config/rustcoon.toml`,
`config/application-entities.toml`, `rustcoon.toml`, or `RUSTCOON__...` environment variables.

See [config/](./config) for example configuration.

## License

Licensed under the terms of the [LICENSE](LICENSE) file.
