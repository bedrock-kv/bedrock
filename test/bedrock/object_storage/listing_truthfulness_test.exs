defmodule Bedrock.ObjectStorage.ListingTruthfulnessTest do
  @moduledoc """
  A listing must never report emptiness it did not verify.

  `ObjectStorage.list/3` returns a bare `Enumerable.t()`, which has no way
  to say "I failed" — a stream can only yield elements or end. So a
  backend that halts on error is indistinguishable from a prefix that is
  genuinely empty, and every consumer downstream reads that silence as
  fact.

  Two consumers depend on the difference, and both are load-bearing:

    * `ChunkReader` reconstructs a contiguous transaction history and
      already promises no silent replay gaps — it raises `ReadError` on a
      header decode failure for exactly that reason. A truncated listing
      breaks that promise underneath it.
    * `Snapshot` decides whether a shard has a durable baseline at all.
      "No snapshot" starts a materializer empty; "couldn't tell" must not.
  """
  use ExUnit.Case, async: true

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.ChunkReader
  alias Bedrock.ObjectStorage.Snapshot

  defmodule FailingBackend do
    @moduledoc false
    @behaviour ObjectStorage

    @impl true
    def list(_config, _prefix, _opts \\ []), do: raise(Bedrock.ObjectStorage.ListError, reason: :econnrefused)

    @impl true
    def get(_config, _key), do: {:error, :econnrefused}
    @impl true
    def put(_config, _key, _data, _opts \\ []), do: {:error, :econnrefused}
    @impl true
    def delete(_config, _key), do: {:error, :econnrefused}
    @impl true
    def put_if_not_exists(_config, _key, _data, _opts \\ []), do: {:error, :econnrefused}
    @impl true
    def get_with_version(_config, _key), do: {:error, :econnrefused}
    @impl true
    def put_if_version_matches(_config, _key, _data, _version, _opts \\ []), do: {:error, :econnrefused}
  end

  @backend {FailingBackend, []}

  describe "ChunkReader keeps its no-silent-gaps promise when the listing fails" do
    test "read_from_version raises rather than reporting an empty history" do
      reader = ChunkReader.new(@backend, "s1")

      # Returning [] here is the data-loss bug: the caller reads it as
      # "this shard has nothing at or above that version" and advances
      # past everything it never received.
      assert_raise ObjectStorage.ListError, fn ->
        reader |> ChunkReader.read_from_version(0, limit: 10) |> Enum.to_list()
      end
    end

    test "list_chunks raises rather than reporting no chunks" do
      reader = ChunkReader.new(@backend, "s1")

      assert_raise ObjectStorage.ListError, fn ->
        reader |> ChunkReader.list_chunks() |> Enum.to_list()
      end
    end
  end

  describe "the failure the lie caused, end to end" do
    test "a shard read reports an error instead of an empty history" do
      # This is the chain that loses data. Before: the listing halts on
      # error -> ChunkReader yields nothing -> get_from_storage returns
      # {:ok, []} -> do_pull falls through to the in-memory buffer ->
      # {:ok, []} -> the materializer's pull_once fabricates a heartbeat
      # at the high-water and advances PAST every version it never
      # received, then serves reads as though it were complete.
      #
      # The fix is only that the listing stops lying. Everything
      # downstream was already correct: an error propagates, the puller
      # fails over, and the circuit breaker retries.
      reader = ChunkReader.new(@backend, "s1")

      result =
        try do
          {:ok, reader |> ChunkReader.read_from_version(0, limit: 100) |> Enum.to_list()}
        rescue
          e -> {:error, e}
        end

      assert {:error, %ObjectStorage.ListError{}} = result
      refute match?({:ok, []}, result), "an unverified empty history is the data-loss bug"
    end
  end

  describe "Snapshot distinguishes absence from ignorance" do
    test "a failed listing is an error, never :not_found" do
      snapshot = Snapshot.new(@backend, "s1")

      # :not_found means "this shard has no durable baseline", which
      # legitimately starts a materializer empty. A backend failure must
      # never be able to say that.
      assert {:error, reason} = Snapshot.read_latest(snapshot)
      refute reason == :not_found
    end
  end
end
