defmodule Bedrock.DataPlane.Materializer.Olivine.IngestTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  defp wait_for_health_report(worker_id, pid, timeout \\ 5_000) do
    receive do
      {:"$gen_cast", {:worker_health, ^worker_id, {:ok, ^pid}}} -> :ok
    after
      timeout -> flunk("Did not receive health report within #{timeout}ms")
    end
  end

  defp start_worker(tmp_dir) do
    worker_id = "ingest-worker-#{System.unique_integer([:positive])}"
    otp_name = :"olivine_ingest_#{System.unique_integer([:positive])}"

    child_spec =
      Olivine.child_spec(
        otp_name: otp_name,
        foreman: self(),
        id: worker_id,
        path: tmp_dir
      )

    {:ok, pid} = start_supervised(child_spec)
    wait_for_health_report(worker_id, pid)
    pid
  end

  defp make_transaction(version_int, key \\ "key") do
    Transaction.encode(%{
      mutations: [{:set, key, "value-#{version_int}"}],
      commit_version: Version.from_integer(version_int)
    })
  end

  defp unlock(pid) do
    epoch = 1
    {:ok, _pid, _info} = GenServer.call(pid, {:lock_for_recovery, epoch})

    :ok = GenServer.call(pid, {:unlock_after_recovery, Version.zero(), []})
  end

  setup do
    tmp_dir = "/tmp/olivine_ingest_#{System.unique_integer([:positive])}"
    File.rm_rf(tmp_dir)
    File.mkdir_p!(tmp_dir)
    on_exit(fn -> File.rm_rf(tmp_dir) end)
    {:ok, tmp_dir: tmp_dir}
  end

  describe "backpressured ingest" do
    test "a small batch is acknowledged immediately and applied", %{tmp_dir: tmp_dir} do
      pid = start_worker(tmp_dir)
      unlock(pid)

      transactions = Enum.map(1_000..1_004, &make_transaction/1)

      assert :ok = GenServer.call(pid, {:ingest, transactions, Version.from_integer(1_004)})

      # The batch lands through the normal apply path: the value is readable.
      v1004 = Version.from_integer(1_004)
      assert {:ok, "value-1004"} = GenServer.call(pid, {:get, "key", v1004, []})
    end

    test "a batch over the high-water parks the reply until the queue drains", %{tmp_dir: tmp_dir} do
      pid = start_worker(tmp_dir)
      unlock(pid)

      # Over the 1_000-transaction high-water in a single handover.
      transactions = Enum.map(1_000..2_100, &make_transaction(&1, "key-#{&1}"))

      # The call must eventually return :ok — after the applier has drained
      # the queue below the release mark. It must not time out.
      assert :ok = GenServer.call(pid, {:ingest, transactions, Version.from_integer(2_100)}, 30_000)
    end

    test "unlock rolls the uncommitted suffix back to the recovery version", %{tmp_dir: tmp_dir} do
      pid = start_worker(tmp_dir)
      unlock(pid)

      v1 = Version.from_integer(1_000)
      v2 = Version.from_integer(2_000)
      v3 = Version.from_integer(3_000)

      transactions = [
        Transaction.encode(%{mutations: [{:set, "key", "a"}], commit_version: v1}),
        Transaction.encode(%{mutations: [{:set, "key", "b"}], commit_version: v2}),
        Transaction.encode(%{mutations: [{:set, "key", "c"}], commit_version: v3})
      ]

      assert :ok = GenServer.call(pid, {:ingest, transactions, v3})
      assert {:ok, "c"} = GenServer.call(pid, {:get, "key", v3, []})

      # Recovery rolls the cluster back to v2: the v3 suffix must vanish.
      {:ok, _pid, _info} = GenServer.call(pid, {:lock_for_recovery, 2})
      :ok = GenServer.call(pid, {:unlock_after_recovery, v2, []})

      assert {:ok, "b"} = GenServer.call(pid, {:get, "key", v2, []})

      # Not merely MVCC at an older version: the v3 entry itself is gone,
      # so a read there is honestly too new rather than answered with "c".
      assert {:error, :version_too_new} = GenServer.call(pid, {:get, "key", v3, [wait_ms: 0]})
    end

    test "ingest while locked acknowledges and discards", %{tmp_dir: tmp_dir} do
      pid = start_worker(tmp_dir)

      # Locked (never unlocked after startup lock)
      {:ok, _pid, _info} = GenServer.call(pid, {:lock_for_recovery, 1})

      assert :ok = GenServer.call(pid, {:ingest, [make_transaction(5_000)], Version.from_integer(5_000)})

      # Nothing was applied: after unlock, the key does not exist at the
      # current (zero) version.
      :ok = GenServer.call(pid, {:unlock_after_recovery, Version.zero(), []})
      assert {:error, :not_found} = GenServer.call(pid, {:get, "key", Version.zero(), []})
    end
  end
end
