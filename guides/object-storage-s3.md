# ObjectStorage S3 Backend

`Bedrock.ObjectStorage.S3` provides an S3-compatible backend for object
storage operations.

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

## Application Config

Set as the default object storage backend:

```elixir
config :bedrock, Bedrock.ObjectStorage,
  backend:
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
