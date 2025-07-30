# Contributing

## Dev Container / Github Codespaces
The easiest way to start contributing is via our Dev Container. This container works both locally in Visual Studio Code as well as [Github Codespaces](https://github.com/features/codespaces). To open the project in vscode you will need the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers). For codespaces you will need to [create a new codespace](https://codespace.new/Mooncake-Labs/pg_mooncake).

With the extension installed you can run the following from the `Command Palette` to get started
```
> Dev Containers: Clone Repository in Container Volume...
```

In the subsequent popup paste the url to the repo and hit enter.
```
https://github.com/Mooncake-Labs/moonlink
```

This will create an isolated Workspace in vscode.

## Testing
Moonlink is a standard Rust project, and tests can be run using cargo test. By default, this will run all tests that don't require optional features.

To run the full test suite, including tests behind optional features such as storage-gcs and storage-s3, you can specify the features explicitly:
```sh
# Run all tests with all supported storage backends
cargo test --all-features
```

Or, run tests with a subset of features:

```sh
# Run tests with GCS support.
cargo test --features storage-gcs

# Run tests with S3 support.
cargo test --features storage-s3
```

## Formatting
Formatting is configured properly in devcontainer via [precommit hooks](https://github.com/Mooncake-Labs/moonlink/blob/main/.pre-commit-config.yaml), please make sure it's triggered before submit a PR.
