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
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.ObjectStorage.Snapshot

  defmodule FailingBackend do
    @moduledoc false
    @behaviour ObjectStorage

    # LAZY, exactly like the real backends: the raise happens when the
    # stream is CONSUMED, not when it is built. That is the property
    # Snapshot.read_latest/1's rescue depends on — an eager stub would
    # pass even if consumption were moved outside the rescued body.
    @impl true
    def list(_config, prefix, _opts \\ []) do
      Stream.resource(
        fn -> :start end,
        fn :start -> raise(Bedrock.ObjectStorage.ListError, reason: :econnrefused, prefix: prefix) end,
        fn _ -> :ok end
      )
    end

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

  describe "the real LocalFilesystem backend, driven into its error path" do
    @describetag :tmp_dir

    setup %{tmp_dir: tmp_dir} do
      on_exit(fn -> File.chmod(Path.join(tmp_dir, "c/locked"), 0o700) end)
      :ok
    end

    test "an unreadable directory raises instead of listing zero keys", %{tmp_dir: tmp_dir} do
      backend = {LocalFilesystem, root: tmp_dir}
      locked = Path.join(tmp_dir, "c/locked")
      File.mkdir_p!(locked)
      File.write!(Path.join(locked, "0001"), "chunk")
      File.chmod!(locked, 0o000)

      assert_raise ObjectStorage.ListError, fn ->
        backend |> ObjectStorage.list("c/locked/") |> Enum.to_list()
      end
    end

    test "a prefix that was never created is EMPTY, not an error", %{tmp_dir: tmp_dir} do
      # The regression this fix must not cause: a fresh cluster listing a
      # shard that has never written a chunk must get [], not a raise.
      backend = {LocalFilesystem, root: tmp_dir}

      assert backend |> ObjectStorage.list("c/never_written/") |> Enum.to_list() == []
    end

    test "one unreadable shard does not break a DIFFERENT shard's listing", %{tmp_dir: tmp_dir} do
      # The walk falls back to the parent when a prefix directory does not
      # exist, so a naive implementation descends into sibling shards. An
      # error there must not be reported as ignorance about THIS shard,
      # whose answer is fully knowable: it has no chunks.
      backend = {LocalFilesystem, root: tmp_dir}
      locked = Path.join(tmp_dir, "c/locked")
      File.mkdir_p!(locked)
      File.write!(Path.join(locked, "0001"), "chunk")
      File.chmod!(locked, 0o000)

      assert backend |> ObjectStorage.list("c/healthy/") |> Enum.to_list() == []
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
    test "a failed listing is an error, never :not_found — even though the raise arrives during consumption" do
      snapshot = Snapshot.new(@backend, "s1")

      # :not_found means "this shard has no durable baseline", which
      # legitimately starts a materializer empty. A backend failure must
      # never be able to say that.
      assert {:error, reason} = Snapshot.read_latest(snapshot)
      refute reason == :not_found
    end
  end
end
