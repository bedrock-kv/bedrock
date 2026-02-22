defmodule Bedrock.Distributed.MinioDurabilityTest do
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Demux.Server
  alias Bedrock.DataPlane.Demux.ShardServer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Keys
  alias Bedrock.ObjectStorage.S3
  alias Bedrock.Test.Minio

  @moduletag :s3
  @moduletag :distributed

  if System.get_env("BEDROCK_MINIO_AVAILABLE") != "1" do
    @moduletag skip: "MinIO not available"
  end

  defmodule FlakyS3Proxy do
    @moduledoc false
    @behaviour ObjectStorage

    @impl true
    def put(config, key, data, opts \\ []), do: ObjectStorage.put(delegate_backend(config), key, data, opts)

    @impl true
    def get(config, key), do: ObjectStorage.get(delegate_backend(config), key)

    @impl true
    def delete(config, key), do: ObjectStorage.delete(delegate_backend(config), key)

    @impl true
    def list(config, prefix, opts \\ []), do: ObjectStorage.list(delegate_backend(config), prefix, opts)

    @impl true
    def put_if_not_exists(config, key, data, opts \\ []) do
      fail_shard_tag = Keyword.fetch!(config, :fail_shard_tag)
      failures = Keyword.fetch!(config, :failures)

      should_fail? =
        String.contains?(key, "/#{fail_shard_tag}/") and
          Agent.get_and_update(failures, fn count ->
            if count == 0 do
              {true, 1}
            else
              {false, count}
            end
          end)

      if should_fail? do
        {:error, :partitioned}
      else
        ObjectStorage.put_if_not_exists(delegate_backend(config), key, data, opts)
      end
    end

    @impl true
    def get_with_version(config, key), do: ObjectStorage.get_with_version(delegate_backend(config), key)

    @impl true
    def put_if_version_matches(config, key, version_token, data, opts \\ []) do
      ObjectStorage.put_if_version_matches(delegate_backend(config), key, version_token, data, opts)
    end

    defp delegate_backend(config), do: Keyword.fetch!(config, :delegate_backend)
  end

  setup do
    bucket = "bedrock-dist-#{:erlang.unique_integer([:positive])}"
    :ok = Minio.initialize_bucket(bucket)
    :ok = Minio.clean_bucket(bucket)

    backend =
      ObjectStorage.backend(S3,
        bucket: bucket,
        config: Minio.config()
      )

    on_exit(fn ->
      Minio.clean_bucket(bucket)
    end)

    {:ok, backend: backend}
  end

  test "3-shard durability watermark advances and survives demux restart", %{backend: backend} do
    {:ok, demux} = start_demux(backend)
    shards = [11, 22, 33]

    for shard <- shards do
      push_txn(demux, shard, 1_000)
      push_txn(demux, shard, 1_200)
    end

    assert_eventually(fn ->
      Server.min_durable_version(demux) == Version.from_integer(1_000)
    end)

    # Advance one shard first: global min should remain bounded by slower shards.
    push_txn(demux, 11, 1_400)
    Process.sleep(25)
    assert Server.min_durable_version(demux) == Version.from_integer(1_000)

    # Advance remaining shards and verify global watermark moves forward.
    push_txn(demux, 22, 1_400)
    push_txn(demux, 33, 1_400)

    assert_eventually(fn ->
      Server.min_durable_version(demux) == Version.from_integer(1_200)
    end)

    # Simulate demux restart and verify persisted replay from object storage.
    Process.exit(demux, :kill)
    Process.sleep(50)

    {:ok, demux_after_restart} = start_demux(backend)

    for shard <- shards do
      {:ok, shard_server} = Server.get_shard_server(demux_after_restart, shard)

      assert_eventually(fn ->
        case ShardServer.pull(shard_server, Version.from_integer(900), timeout: 200, limit: 10) do
          {:ok, txns} ->
            versions = Enum.map(txns, fn {version, _slice} -> Version.to_integer(version) end)
            1_000 in versions and 1_200 in versions

          _ ->
            false
        end
      end)
    end
  end

  test "transient shard partition heals via retry and advances durability", %{backend: backend} do
    {:ok, failures} = Agent.start_link(fn -> 0 end)
    on_exit(fn -> if Process.alive?(failures), do: Agent.stop(failures) end)
    shard_with_partition = 22

    flaky_backend =
      ObjectStorage.backend(FlakyS3Proxy,
        delegate_backend: backend,
        fail_shard_tag: Keys.shard_tag(shard_with_partition),
        failures: failures
      )

    {:ok, demux} = start_demux(flaky_backend)

    for shard <- [11, shard_with_partition, 33] do
      push_txn(demux, shard, 1_000)
      push_txn(demux, shard, 1_200)
    end

    assert_eventually(fn ->
      Server.min_durable_version(demux) == Version.from_integer(1_000)
    end)

    assert Agent.get(failures, & &1) == 1
  end

  defp start_demux(backend) do
    Server.start_link(
      cluster: "distributed-test-cluster",
      object_storage: backend,
      log: self(),
      shard_server_opts: [
        version_gap: 100,
        persistence_retry_backoff_ms: 1,
        persistence_retry_tick_ms: 1
      ]
    )
  end

  defp push_txn(demux, shard_id, version_int) do
    version = Version.from_integer(version_int)

    txn =
      Transaction.encode(%{
        mutations: [{:set, "k:#{shard_id}:#{version_int}", "v"}],
        shard_index: [{shard_id, 1}],
        commit_version: version
      })

    :ok = Server.push(demux, version, txn)
  end

  defp assert_eventually(fun, timeout_ms \\ 3_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    eventually_loop(fun, deadline)
  end

  defp eventually_loop(fun, deadline) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(25)
        eventually_loop(fun, deadline)
      else
        flunk("condition not met before timeout")
      end
    end
  end
end
