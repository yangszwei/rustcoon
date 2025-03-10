FROM rust:1.85.0-alpine3.21 AS builder
WORKDIR /app

# Install necessary build tools
RUN apk add --no-cache build-base cmake

# Create an empty main file to allow dependency fetching
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Fetch dependencies (cacheable if Cargo.toml and Cargo.lock don’t change)
COPY Cargo.toml Cargo.lock ./
RUN cargo fetch

# Copy the application code
COPY migrations migrations
COPY src src

# Build the application in release mode
RUN cargo build --release

FROM alpine:3.21 AS runner
WORKDIR /app

# Set environment variables for data path
ENV DATA_DIR=/data

# Copy the compiled binary from the builder stage
COPY --from=builder /app/target/release/rustcoon /app/rustcoon

# Expose the port the application will listen on
EXPOSE 3000

ENTRYPOINT ["/app/rustcoon"]
