# Rustcoon

## Overview

Rustcoon is a lightweight DICOM PACS server built in Rust, inspired by the [cylab-tw/raccoon-dicom](https://github.com/cylab-tw/raccoon-dicom) project. It focuses on simplicity, ease of use, and observability, providing a solution for working with DICOM in non-production environments such as development, testing, or research.

This project started as a personal learning experience with Rust but aims to be useful for others who need a simple DICOM server implementation.

> **Note:** This project is still in the early stages of development and should not be used in production environments.

## Features

- DICOMweb Support: QIDO-RS, WADO-RS, STOW-RS
- Image Rendering Support (Very unstable, with a high chance of rendering failures)
- Supports PostgreSQL & SQLite databases
- Customizable server and storage settings

## Running the Application

### Prerequisites

- Rust (2021 edition)
- PostgreSQL Database (optional)

### Running the Application

```shell
cargo run --release
```

## Configuration

The application can be configured via environment variables or command-line flags. For a list of available options, check the [.env.example](.env.example) file or run the application with `--help`.

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).
