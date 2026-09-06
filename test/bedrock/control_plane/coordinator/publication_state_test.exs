defmodule Bedrock.ControlPlane.Coordinator.PublicationStateTest do
  use ExUnit.Case, async: true

  alias Bedrock.ClusterBootstrap.Publication
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.SystemKeys.ClusterBootstrap

  @moduletag :tmp_dir

  defmodule LostReply do
    @moduledoc false
    def get_with_version(c, k), do: LocalFilesystem.get_with_version(c, k)

    def put_if_version_matches(c, k, token, bytes, opts) do
      case LocalFilesystem.put_if_version_matches(c, k, token, bytes, opts) do
        :ok -> {:error, :timeout}
        error -> error
      end
    end
  end

  defmodule AdversarialBackend do
    @moduledoc false
    def get_with_version(config, key) do
      {mode, written?} = Agent.get(Keyword.fetch!(config, :state), & &1)

      case {mode, written?} do
        {:unavailable, true} -> {:error, :unavailable}
        {:missing, true} -> {:error, :not_found}
        _ -> LocalFilesystem.get_with_version(config, key)
      end
    end

    def put_if_version_matches(config, key, token, bytes, opts) do
      state = Keyword.fetch!(config, :state)
      {mode, written?} = Agent.get(state, & &1)
      send(Keyword.fetch!(config, :owner), :cas_attempt)
      Agent.update(state, fn _ -> {mode, true} end)

      case mode do
        :conflicts ->
          {:error, :version_mismatch}

        :lower_wins ->
          if !written? do
            {lower, final} = Keyword.fetch!(config, :lower_publication)
            :ok = Publication.publish(lower, final)
          end

          LocalFilesystem.put_if_version_matches(config, key, token, bytes, opts)

        :higher_after_write ->
          :ok = LocalFilesystem.put_if_version_matches(config, key, token, bytes, opts)
          {:ok, _} = Publication.reserve({LocalFilesystem, config}, key, 9, "higher", "matrix")
          :ok

        :different_payload ->
          {:ok, candidate} = ClusterBootstrap.read(bytes)
          changed = ClusterBootstrap.to_binary(%{candidate | logs: [%{id: "foreign-payload"}]})
          :ok = LocalFilesystem.put_if_version_matches(config, key, token, changed, opts)
          {:error, :timeout}

        _ ->
          {:error, :timeout}
      end
    end
  end

  defmodule MissingToken do
    @moduledoc false
    def get_with_version(config, key) do
      {:ok, bytes, _token} = LocalFilesystem.get_with_version(config, key)
      {:ok, bytes, ""}
    end
  end

  test "coherent read requires a nonempty body-associated token", %{root: root} do
    assert {:error, :missing_version_token} = Publication.read({MissingToken, root: root}, "bootstrap")
  end

  defmodule MissingBackend do
    @moduledoc false
    def node_config, do: []
  end

  setup %{tmp_dir: root} do
    initial = %{
      cluster_id: "matrix",
      epoch: 7,
      logs: [%{id: "old", shard_tags: [0, 1]}],
      coordinators: [%{node: "n@host"}]
    }

    backend = {LocalFilesystem, root: root}
    :ok = ObjectStorage.put(backend, "bootstrap", ClusterBootstrap.to_binary(initial))
    %{initial: initial, backend: backend, root: root}
  end

  test "accepted migration table preserves completed legacy identity absence", %{initial: initial} do
    for epoch <- [0, 1] do
      fresh = %{initial | epoch: epoch, logs: []}
      assert :ok = Publication.validate(fresh)
      reserved = Map.merge(fresh, %{protocol_version: 1, recovery_generation: 3, recovery_id: "new"})
      assert :ok = Publication.validate(reserved)
      assert {:error, :invalid_bootstrap} = Publication.validate(Map.put(reserved, :publication_id, "new"))
    end

    assert :ok = Publication.validate(initial)
    migrated = Map.merge(initial, %{protocol_version: 1, recovery_generation: 8, recovery_id: "new"})
    assert :ok = Publication.validate(migrated)
    assert :ok = Publication.validate(Map.put(migrated, :publication_id, "old"))

    for invalid <- [
          Map.put(migrated, :publication_id, "new"),
          Map.put(migrated, :recovery_generation, 7),
          Map.put(migrated, :recovery_generation, 6),
          Map.put(migrated, :recovery_id, "")
        ] do
      assert {:error, :invalid_bootstrap} = Publication.validate(invalid)
    end

    completed = Map.merge(migrated, %{epoch: 8, publication_id: "new"})
    assert :ok = Publication.validate(completed)
    assert {:error, :invalid_bootstrap} = Publication.validate(Map.put(completed, :publication_id, "foreign"))

    assert {:error, {:unsupported_bootstrap_protocol, 2}} =
             Publication.validate(Map.put(completed, :protocol_version, 2))

    assert {:error, :no_object_storage} = Publication.location(MissingBackend)
  end

  test "unknown reservation CAS result retains exact coherent token", %{root: root, backend: backend} do
    assert {:ok, reservation} = Publication.reserve({LostReply, root: root}, "bootstrap", 8, "reserve8", "matrix")
    assert {:ok, bytes, token} = ObjectStorage.get_with_version(backend, "bootstrap")
    assert reservation.version_token == token
    assert reservation.reserved_bytes == bytes
    assert reservation.prior_bootstrap.epoch == 7
    assert reservation.prior_bootstrap[:publication_id] in [nil, ""]
  end

  test "lower final wins before higher reservation and its completed config is retained", %{backend: backend} do
    {:ok, lower} = Publication.reserve(backend, "bootstrap", 8, "lower", "matrix")

    final =
      Map.merge(lower.prior_bootstrap, %{
        protocol_version: 1,
        epoch: 8,
        logs: [%{id: "winner"}],
        recovery_generation: 8,
        recovery_id: "lower",
        publication_id: "lower",
        parameters: %{desired_logs: 3}
      })

    assert :ok = Publication.publish(lower, final)
    {:ok, higher} = Publication.reserve(backend, "bootstrap", 9, "higher", "matrix")
    assert higher.prior_bootstrap.epoch == 8
    assert higher.prior_bootstrap.logs == [%{id: "winner"}]
    assert higher.prior_bootstrap.parameters.desired_logs == 3
    assert higher.prior_bootstrap.publication_id == "lower"
    assert {:error, :publication_mismatch} = Publication.publish(lower, final)
  end

  test "older final publication between higher read and CAS is preserved on conflict retry", %{
    backend: backend,
    root: root
  } do
    {:ok, lower} = Publication.reserve(backend, "bootstrap", 8, "lower", "matrix")

    final =
      Map.merge(lower.prior_bootstrap, %{
        protocol_version: 1,
        epoch: 8,
        logs: [%{id: "published-between-read-and-cas"}],
        system_materializers: [%{id: "member", node: "n@host"}],
        parameters: %{desired_logs: 3},
        policies: %{allow_volunteer_nodes_to_join: false},
        recovery_generation: 8,
        recovery_id: "lower",
        publication_id: "lower"
      })

    {module, options} = adversarial(root, :lower_wins)
    higher_backend = {module, Keyword.put(options, :lower_publication, {lower, final})}
    assert {:ok, higher} = Publication.reserve(higher_backend, "bootstrap", 9, "higher", "matrix")
    assert higher.prior_bootstrap == elem(ClusterBootstrap.read(ClusterBootstrap.to_binary(final)), 1)
    assert_received :cas_attempt
    assert_received :cas_attempt
    refute_received :cas_attempt
    assert higher.prior_bootstrap.parameters.desired_logs == 3
    assert higher.prior_bootstrap.system_materializers == [%{id: "member", node: "n@host"}]
  end

  test "higher reservation between successful CAS and exact readback prevents activation", %{root: root} do
    backend = adversarial(root, :higher_after_write)

    assert {:error, {:reservation_unverified, :publication_mismatch}} =
             Publication.reserve(backend, "bootstrap", 8, "lower", "matrix")

    assert {:ok, current} = Publication.read(backend, "bootstrap")
    assert current.bootstrap.recovery_generation == 9
    assert current.bootstrap.recovery_id == "higher"
  end

  test "reservation conflicts stop after exactly three CAS attempts", %{root: root} do
    backend = adversarial(root, :conflicts)
    assert {:error, :reservation_conflicts} = Publication.reserve(backend, "bootstrap", 8, "lower", "matrix")
    for _ <- 1..3, do: assert_received(:cas_attempt)
    refute_received :cas_attempt
  end

  for {mode, expected} <- [different_payload: :publication_mismatch, unavailable: :unavailable, missing: :not_found] do
    test "final verification fails closed for #{mode}", %{root: root, backend: backend} do
      {:ok, reserved} = Publication.reserve(backend, "bootstrap", 8, "same-id", "matrix")

      final =
        Map.merge(reserved.prior_bootstrap, %{
          protocol_version: 1,
          epoch: 8,
          recovery_generation: 8,
          recovery_id: "same-id",
          publication_id: "same-id"
        })

      reserved = %{reserved | backend: adversarial(root, unquote(mode))}
      assert {:error, unquote(expected)} = Publication.publish(reserved, final)
      assert_received :cas_attempt
      refute_received :cas_attempt
    end
  end

  test "legacy mixed fields and uint64 overflow fail closed", %{initial: initial} do
    for invalid <- [
          Map.put(initial, :recovery_id, "mixed"),
          Map.put(initial, :publication_id, "mixed"),
          Map.put(initial, :recovery_generation, 8),
          Map.put(initial, :epoch, -1),
          Map.put(initial, :epoch, 0x10000000000000000)
        ] do
      assert {:error, :invalid_bootstrap} = Publication.validate(invalid)
    end

    max = 0xFFFFFFFFFFFFFFFF

    completed =
      Map.merge(initial, %{
        protocol_version: 1,
        epoch: max,
        recovery_generation: max,
        recovery_id: "max",
        publication_id: "max"
      })

    assert :ok = Publication.validate(completed)
    assert {:error, :invalid_bootstrap} = Publication.validate(Map.put(completed, :recovery_generation, max + 1))
  end

  defp adversarial(root, mode) do
    state = start_supervised!({Agent, fn -> {mode, false} end})
    {AdversarialBackend, root: root, state: state, owner: self()}
  end

  test "two concurrent reservations converge to the higher generation", %{backend: backend} do
    owner = self()

    tasks =
      for generation <- [8, 9] do
        Task.async(fn ->
          send(owner, {:ready, self()})

          receive do
            :go -> Publication.reserve(backend, "bootstrap", generation, "r#{generation}", "matrix")
          end
        end)
      end

    for task <- tasks do
      pid = task.pid
      assert_receive {:ready, ^pid}
    end

    Enum.each(tasks, &send(&1.pid, :go))
    [lower, higher] = Enum.map(tasks, &Task.await/1)
    assert {:ok, reservation9} = higher
    assert {:ok, current} = Publication.read(backend, "bootstrap")
    assert current.bootstrap.recovery_generation == 9
    assert current.bootstrap.logs == [%{id: "old", shard_tags: [0, 1]}]

    if match?({:ok, _}, lower) do
      {:ok, reservation8} = lower

      final8 =
        Map.merge(reservation8.prior_bootstrap, %{
          protocol_version: 1,
          epoch: 8,
          recovery_generation: 8,
          recovery_id: "r8",
          publication_id: "r8"
        })

      assert {:error, :publication_mismatch} = Publication.publish(reservation8, final8)
    else
      assert lower in [
               {:error, :superseded},
               {:error, {:reservation_unverified, :publication_mismatch}}
             ]
    end

    final9 =
      Map.merge(reservation9.prior_bootstrap, %{
        protocol_version: 1,
        epoch: 9,
        recovery_generation: 9,
        recovery_id: "r9",
        publication_id: "r9"
      })

    assert :ok = Publication.publish(reservation9, final9)
    # Lost notification retries use the same immutable reservation, not a new token.
    assert :ok = Publication.publish(reservation9, final9)
    assert {:ok, completed} = Publication.read(backend, "bootstrap")
    assert completed.bootstrap.publication_id == "r9"
  end
end
