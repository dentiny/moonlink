# TODO(hjiang): Use release build for docker images, update after official release.

# Build base image for compilation and linking.
FROM mcr.microsoft.com/devcontainers/rust:latest AS builder

WORKDIR /workspace

COPY . .

RUN apt-get update && apt-get install -y gcc-x86-64-linux-gnu
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc
RUN rustup target add x86_64-unknown-linux-gnu
RUN cargo build --target x86_64-unknown-linux-gnu --verbose

# Use a slim image for docker image used for cloud deployment.
FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y ca-certificates libssl3 libpq5 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /workspace/target/x86_64-unknown-linux-gnu/debug/moonlink_service .
