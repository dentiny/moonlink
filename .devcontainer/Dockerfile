FROM mcr.microsoft.com/devcontainers/rust:latest

RUN apt update \
 && apt install -y python3-pip \
 && apt install -y mold \
 && sudo ln -fs /usr/bin/mold /usr/bin/ld \
 && rm -rf /var/lib/apt/lists/*

USER vscode

RUN cargo install cargo-deny cargo-sort

RUN pip3 install --break-system-packages pre-commit
