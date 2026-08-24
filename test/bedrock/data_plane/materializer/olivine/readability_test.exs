defmodule Bedrock.DataPlane.Materializer.Olivine.ReadabilityTest do
  @moduledoc """
  A materializer answers questions it has the answers to, and refuses the
  ones it doesn't — FoundationDB's storage-server contract.

  FDB tracks per-shard state (`storageserver.actor.cpp:470`) and gates
  reads on it: `isReadable()` is `readWrite != nullptr` (`:558`), and a
  shard still fetching gets `wrong_shard_server` at the request boundary
  (`:2994`). It never ranks itself against peers and never guesses; it
  knows locally whether it can serve, and says so.

  Our evidence for "I cannot serve this" is the log's own answer:
  `{:error, {:version_too_old, floor}}` means the cluster's retention
  floor sits above where this worker's data ends, so the span between is
  unreachable from here. Anything it returned for those keys would be
  absence-shaped and wrong.
  """
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Materializer.Olivine
  alias Bedrock.DataPlane.Version

  @moduletag :tmp_dir

  defp start_worker(tmp_dir) do
    worker_id = "readable_wkr_#{System.unique_integer([:positive])}"
    otp_name = :"olivine_readable_#{System.unique_integer([:positive])}"

    child_spec =
      Olivine.child_spec(otp_name: otp_name, foreman: self(), id: worker_id, path: tmp_dir, params: %{})

    {GenServer, :start_link, args} = child_spec.start
    {:ok, pid} = apply(GenServer, :start, args)

    receive do
      {:"$gen_cast", {:worker_health, ^worker_id, {:ok, ^pid}}} -> :ok
    after
      5_000 -> flunk("no health report")
    end

    :sys.replace_state(pid, &%{&1 | mode: :running})
    pid
  end

  describe "a worker that cannot account for its shard's history refuses reads" do
    test "a get is refused with :unavailable, never answered as an absent key", %{tmp_dir: tmp_dir} do
      pid = start_worker(tmp_dir)
      v = Version.from_integer(0)

      # Before the hole is known the worker ANSWERS: it believes it holds
      # the shard, so it reports the key as absent.
      assert {:error, :not_found} = GenServer.call(pid, {:get, "k", v, [timeout: 100]}, 500)

      send(pid, {:shard_hole, Version.from_integer(9000)})

      # After it, the SAME read must not answer. An absence reply here
      # would be a wrong answer dressed as a right one: the key may well
      # exist below the retention floor this worker cannot reach.
      assert {:error, :unavailable} = GenServer.call(pid, {:get, "k", v, [timeout: 100]}, 500)
    end

    test "a get_range is refused too — the same data is missing either way", %{tmp_dir: tmp_dir} do
      pid = start_worker(tmp_dir)
      v = Version.from_integer(0)

      send(pid, {:shard_hole, Version.from_integer(9000)})

      assert {:error, :unavailable} = GenServer.call(pid, {:get_range, "a", "z", v, [timeout: 100]}, 500)
    end
  end
end
