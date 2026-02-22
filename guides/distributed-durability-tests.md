# Distributed Durability Test Suite

Bedrock includes a MinIO-backed distributed durability suite for foundational
3-shard scenarios.

Test module:

- `test/bedrock/distributed/minio_durability_test.exs`

Tags:

- `:s3`
- `:distributed`

## Local Execution

1. Ensure MinIO test binaries are installed:

```bash
MIX_ENV=test mix minio_server.download --arch darwin-arm64 --version latest
```

2. Run the distributed suite explicitly:

```bash
BEDROCK_INCLUDE_DISTRIBUTED=1 mix test --include s3 --include distributed test/bedrock/distributed/minio_durability_test.exs
```

By default, `:distributed` tests are excluded unless explicitly enabled.

## CI Execution

The CI workflow runs distributed durability tests in a dedicated job on
scheduled/manual runs with MinIO setup. This keeps default PR pipelines fast
while preserving regular distributed durability coverage.
