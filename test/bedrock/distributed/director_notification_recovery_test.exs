defmodule Bedrock.Distributed.DirectorNotificationRecoveryTest do
  use ExUnit.Case, async: false

  alias Bedrock.ClusterBootstrap.Publication

  defmodule Cluster do
    @moduledoc false
    use Bedrock.Cluster, otp_app: :bedrock, name: "director_notification_recovery"
  end

  defmodule Repo do
    use Bedrock.Repo, cluster: Cluster
  end

  @moduletag :distributed
  @moduletag timeout: 60_000

  test "an old publication cannot restore retired routing after real Director replacement" do
    assert Node.alive?()
    root = Path.join(System.tmp_dir!(), "director-notification-#{System.system_time(:nanosecond)}")
    previous = Application.get_env(:bedrock, Cluster)
    previous_storage = Application.get_env(:bedrock, Bedrock.ObjectStorage)
    backend = {Bedrock.ObjectStorage.LocalFilesystem, root: Path.join(root, "objects")}
    Application.put_env(:bedrock, Bedrock.ObjectStorage, backend: backend)

    Application.put_env(:bedrock, Cluster,
      capabilities: [:coordination, :log, :materializer],
      durability_mode: :relaxed,
      path_to_descriptor: Path.join(root, "descriptor"),
      object_storage: backend,
      coordinator: [path: root],
      materializer: [path: root, object_storage: backend],
      log: [path: root, object_storage: backend]
    )

    on_exit(fn ->
      restore(Cluster, previous)
      restore(Bedrock.ObjectStorage, previous_storage)
    end)

    start_supervised!({Cluster, []})
    assert :ok = Repo.transact(fn -> Repo.put("notification/prefix", "acknowledged") end, timeout_in_ms: 15_000)
    assert "acknowledged" = Repo.transact(fn -> Repo.get("notification/prefix") end, timeout_in_ms: 15_000)
    coordinator = Process.whereis(Cluster.otp_name(:coordinator))
    initial = :sys.get_state(coordinator)

    recovered =
      Enum.reduce(1..2, initial, fn _failure, prior ->
        down = Process.monitor(prior.director)
        Process.exit(prior.director, :kill)
        assert_receive {:DOWN, ^down, :process, _, :killed}, 5_000
        next = wait_for_replacement(coordinator, prior, System.monotonic_time(:millisecond) + 15_000)
        assert next.epoch > prior.epoch
        assert next.raft_term == prior.raft_term
        assert next.bootstrap_reservation.recovery_id != prior.bootstrap_reservation.recovery_id
        assert next.last_allocation.generation == next.epoch
        assert {:ok, durable} = Publication.read(backend, "bootstrap")
        assert durable.bootstrap.epoch == next.epoch
        assert durable.bootstrap.publication_id == next.bootstrap_reservation.recovery_id
        assert "acknowledged" = Repo.transact(fn -> Repo.get("notification/prefix") end, timeout_in_ms: 15_000)
        next
      end)

    GenServer.cast(
      coordinator,
      {:notify_transaction_system_layout, initial.transaction_system_layout, initial.prior_core_state}
    )

    GenServer.cast(
      coordinator,
      {:notify_transaction_system_layout, {initial.director, initial.epoch, 999}, initial.transaction_system_layout,
       initial.prior_core_state}
    )

    GenServer.cast(coordinator, {:notify_config, {initial.director, initial.epoch, 999}, %{stale: true}})
    after_delayed = :sys.get_state(coordinator)
    artifact = Path.join(root, "delayed-publication.term")

    File.write!(
      artifact,
      :erlang.term_to_binary(%{initial: initial, recovered: recovered, after_delayed: after_delayed})
    )

    IO.puts("Notification regression artifact: #{artifact}")
    assert after_delayed.transaction_system_layout == recovered.transaction_system_layout
    assert after_delayed.prior_core_state == recovered.prior_core_state
    assert after_delayed.epoch == recovered.epoch
    assert after_delayed.config == recovered.config
    assert "acknowledged" = Repo.transact(fn -> Repo.get("notification/prefix") end, timeout_in_ms: 15_000)
  end

  defp wait_for_replacement(coordinator, initial, deadline) do
    state = :sys.get_state(coordinator)

    if is_pid(state.director) and state.director != initial.director and
         state.transaction_system_layout != nil and
         state.transaction_system_layout.logs != initial.transaction_system_layout.logs do
      state
    else
      assert System.monotonic_time(:millisecond) < deadline, "replacement did not publish: #{inspect(state)}"
      Process.sleep(10)
      wait_for_replacement(coordinator, initial, deadline)
    end
  end

  defp restore(key, nil), do: Application.delete_env(:bedrock, key)
  defp restore(key, value), do: Application.put_env(:bedrock, key, value)
end
