defmodule Bedrock.Distributed.MinioDurabilityTest do
  @moduledoc """
  The distributed durability gate, against a real MinIO backend
  (bedrock-qzr.20).

  Exercises the Demux cut protocol end to end: cuts are deterministic
  version-bucket boundaries, a cut candidate fires only once the
  known-committed version reaches it, every shard's confirmed cut is its
  floor contribution, and the global watermark is the minimum over shards.
  Chunks written through a demux's lifetime replay from object storage
  after a restart, and a transient write failure heals through the
  persistence worker's retry without losing the cut.

  Run with:

      BEDROCK_INCLUDE_DISTRIBUTED=1 mix test --include distributed \\
        test/bedrock/distributed/minio_durability_test.exs
  """
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Demux.Server
  alias Bedrock.DataPlane.Demux.ShardServer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Keys
  alias Bedrock.ObjectStorage.S3
  alias Bedrock.Test.Minio

  @moduletag :distributed

  # Small version-time buckets so test versions (microsecond integers in
  # the low thousands) cross cut boundaries; the default five-second
  # interval would never be crossed and no cut would ever fire.
  @cut_interval_us 100

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
    shard_base = :erlang.unique_integer([:positive]) * 1_000
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

    {:ok, backend: backend, shard_base: shard_base}
  end

  test "3-shard durability watermark advances behind KCV and survives demux restart", %{
    backend: backend,
    shard_base: shard_base
  } do
    {:ok, demux} = start_demux(backend)
    [shard_a, shard_b, shard_c] = shards = [shard_base + 11, shard_base + 22, shard_base + 33]

    # Bucket 10 (interval 100): one transaction per shard, in global commit
    # order, KCV trailing one commit behind — the shape the commit proxies
    # produce.
    push_txn(demux, shard_a, 1_010, nil)
    push_txn(demux, shard_b, 1_020, 1_010)
    push_txn(demux, shard_c, 1_030, 1_020)

    # Crossing into bucket 12 proposes the cut at 1_199 — a CANDIDATE only.
    # The KCV (1_030) has not reached it: nothing may become durable.
    push_txn(demux, shard_a, 1_210, 1_030)
    Process.sleep(100)

    # Shards exist but nothing is confirmed: the floor is honestly zero.
    premature = Server.min_durable_version(demux)

    assert premature == Version.zero(),
           "cut advanced before the known-committed version reached it: #{inspect(premature)}"

    # The KCV passes the cut: it fires, every shard flushes its bucket-10
    # data and confirms, and the global minimum is the confirmed cut.
    push_txn(demux, shard_b, 1_220, 1_210)

    assert_eventually(
      fn -> Server.min_durable_version(demux) == Version.from_integer(1_199) end,
      3_000,
      fn -> "watermark did not reach 1_199; at #{inspect(Server.min_durable_version(demux))}" end
    )

    # Next bucket, same discipline: the candidate at 1_299 waits for the KCV…
    push_txn(demux, shard_c, 1_230, 1_220)
    push_txn(demux, shard_a, 1_310, 1_230)
    Process.sleep(100)

    assert Server.min_durable_version(demux) == Version.from_integer(1_199),
           "cut advanced before the known-committed version reached it"

    # …and moves the watermark forward once the KCV passes it.
    push_txn(demux, shard_b, 1_320, 1_310)

    assert_eventually(
      fn -> Server.min_durable_version(demux) == Version.from_integer(1_299) end,
      3_000,
      fn -> "watermark did not reach 1_299; at #{inspect(Server.min_durable_version(demux))}" end
    )

    # Restart: a fresh demux over the same object storage replays every
    # flushed transaction from MinIO chunks.
    Process.exit(demux, :kill)
    Process.sleep(50)

    {:ok, demux_after_restart} = start_demux(backend)

    expected_by_shard = %{shard_a => [1_010, 1_210], shard_b => [1_020, 1_220], shard_c => [1_030, 1_230]}

    for shard <- shards do
      {:ok, shard_server} = Server.get_shard_server(demux_after_restart, shard)
      expected = Map.fetch!(expected_by_shard, shard)

      assert_eventually(
        fn ->
          case ShardServer.pull(shard_server, Version.from_integer(900), timeout: 200, limit: 10) do
            {:ok, txns, _currency} ->
              versions = Enum.map(txns, fn {version, _slice} -> Version.to_integer(version) end)
              Enum.all?(expected, &(&1 in versions))

            _ ->
              false
          end
        end,
        8_000,
        fn -> "shard #{shard} did not replay #{inspect(expected)} from MinIO chunks" end
      )
    end
  end

  test "transient shard partition heals via retry and advances durability", %{
    backend: backend,
    shard_base: shard_base
  } do
    {:ok, failures} = Agent.start_link(fn -> 0 end)
    on_exit(fn -> if Process.alive?(failures), do: Agent.stop(failures) end)
    [shard_a, shard_b, shard_c] = [shard_base + 11, shard_base + 22, shard_base + 33]

    flaky_backend =
      ObjectStorage.backend(FlakyS3Proxy,
        delegate_backend: backend,
        fail_shard_tag: Keys.shard_tag(shard_b),
        failures: failures
      )

    {:ok, demux} = start_demux(flaky_backend)

    push_txn(demux, shard_a, 1_010, nil)
    push_txn(demux, shard_b, 1_020, 1_010)
    push_txn(demux, shard_c, 1_030, 1_020)

    # Fire the cut at 1_199: shard B's first chunk write fails once, the
    # persistence worker retries, and the confirmed cut still reaches the
    # global minimum — the partition cost latency, never durability.
    push_txn(demux, shard_a, 1_210, 1_030)
    push_txn(demux, shard_b, 1_220, 1_210)

    assert_eventually(
      fn -> Server.min_durable_version(demux) == Version.from_integer(1_199) end,
      5_000,
      fn -> "watermark did not heal to 1_199; at #{inspect(Server.min_durable_version(demux))}" end
    )

    assert Agent.get(failures, & &1) >= 1,
           "the flaky backend never failed a write: the retry path was not exercised"
  end

  defp start_demux(backend) do
    child_spec =
      Supervisor.child_spec(
        {Server,
         cluster: "distributed-test-cluster",
         object_storage: backend,
         log: self(),
         cut_interval_us: @cut_interval_us,
         shard_server_opts: [
           persistence_retry_backoff_ms: 1,
           persistence_retry_tick_ms: 1
         ]},
        restart: :temporary
      )

    {:ok, start_supervised!(child_spec)}
  end

  defp push_txn(demux, shard_id, version_int, kcv_int) do
    version = Version.from_integer(version_int)
    kcv = if kcv_int, do: Version.from_integer(kcv_int)

    txn =
      Transaction.encode(%{
        mutations: [{:set, "k:#{shard_id}:#{version_int}", "v"}],
        shard_index: [{shard_id, 1}],
        commit_version: version
      })

    :ok = Server.push(demux, version, txn, kcv)
  end

  defp assert_eventually(fun, timeout_ms, describe_fn) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    eventually_loop(fun, deadline, describe_fn)
  end

  defp eventually_loop(fun, deadline, describe_fn) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(25)
        eventually_loop(fun, deadline, describe_fn)
      else
        flunk(describe_fn.())
      end
    end
  end
end
