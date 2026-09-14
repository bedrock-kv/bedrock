# ObjectStorage S3 Backend

`Bedrock.ObjectStorage.S3` provides an S3-compatible backend for object
storage operations.

For cross-component durability behavior (profiles, queueing, watermarking,
trim safety, and migration notes), see `guides/durability-foundation.md`.

## Backend Configuration

Configure via `Bedrock.ObjectStorage.backend/2`:

```elixir
backend =
  Bedrock.ObjectStorage.backend(Bedrock.ObjectStorage.S3,
    bucket: "bedrock",
    config: [
      access_key_id: "minio_key",
      secret_access_key: "minio_secret",
      scheme: "http://",
      region: "local",
      host: "127.0.0.1",
      port: 9000
    ]
  )
```

## Node Config

Set the backend at the top level of the cluster's node config:

```elixir
config :my_app, MyApp.Cluster,
  object_storage:
    {Bedrock.ObjectStorage.S3,
     [
       bucket: "bedrock",
       config: [
         access_key_id: "minio_key",
         secret_access_key: "minio_secret",
         scheme: "http://",
         region: "local",
         host: "127.0.0.1",
         port: 9000
       ]
     ]}
```

You can also use shorthand backend selection with top-level S3 options:

```elixir
config :my_app, MyApp.Cluster,
  object_storage: :s3,
  s3: [
    bucket: "bedrock",
    access_key_id: "minio_key",
    secret_access_key: "minio_secret",
    scheme: "http://",
    region: "local",
    host: "127.0.0.1",
    port: 9000
  ]
```

Workers, the director, and the coordinator all resolve the backend the same
way: top-level `object_storage:`, else `<first role path>/object_storage`.
`object_storage:` inside a role section (`log:`, `materializer:`, ...) and
`config :bedrock, Bedrock.ObjectStorage, backend: ...` both fail startup. In a
multi-node cluster every node must name the same shared store, or each node
keeps its own bootstrap.

## Conditional Semantics

`Bedrock.ObjectStorage.S3` uses native S3 preconditions for optimistic
concurrency and conditional create:

- `put_if_not_exists/4` uses `If-None-Match: *` and returns
  `{:error, :already_exists}` when the key already exists.
- `get_with_version/2` returns an opaque version token derived from the
  object's `ETag`.
- `put_if_version_matches/5` uses `If-Match` with the token from
  `get_with_version/2` and returns `{:error, :version_mismatch}` for stale
  tokens.

These semantics are validated against MinIO in tests tagged `:s3`.
