defmodule Bedrock.ControlPlane.Director.Recovery.BootstrapReservationTest do
  use ExUnit.Case, async: false

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ClusterBootstrap.Discovery
  alias Bedrock.ClusterBootstrap.Publication
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Director.Recovery.PersistencePhase
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.SystemKeys.ClusterBootstrap

  @moduletag :tmp_dir

  defmodule Cluster do
    @moduledoc false
    def name, do: "bootstrap_reservation"
    def node_config, do: Application.fetch_env!(:bedrock, __MODULE__)
  end

  setup %{tmp_dir: root} do
    backend = {LocalFilesystem, root: root}
    Application.put_env(:bedrock, Cluster, object_storage: backend)
    on_exit(fn -> Application.delete_env(:bedrock, Cluster) end)

    initial = %{
      cluster_id: "reservation-test",
      epoch: 7,
      logs: [%{id: "committed-7"}],
      coordinators: [%{node: Atom.to_string(node())}]
    }

    :ok = ObjectStorage.put(backend, "bootstrap", ClusterBootstrap.to_binary(initial))
    {:ok, initial_bytes, _token} = ObjectStorage.get_with_version(backend, "bootstrap")

    {:ok, reservation} =
      Publication.reserve(backend, "bootstrap", 8, "request-8", initial.cluster_id)

    %{
      backend: backend,
      initial: initial,
      initial_bytes: initial_bytes,
      token: reservation.version_token,
      reservation: reservation
    }
  end

  test "a delayed recovery cannot refresh its final token and overwrite newer bootstrap", fixture do
    %{backend: backend, initial: initial, token: token} = fixture
    newer = %{initial | epoch: 9, logs: [%{id: "committed-9"}]}
    newer_bytes = ClusterBootstrap.to_binary(newer)
    :ok = ObjectStorage.put_if_version_matches(backend, "bootstrap", token, newer_bytes)
    attempt = attempt(8)
    context = context(fixture)
    result = PersistencePhase.execute(attempt, context)
    assert {:ok, bytes} = ObjectStorage.get(backend, "bootstrap")

    assert bytes == newer_bytes,
           "stale epoch8 overwrote committed epoch9 after reading a fresh token: #{inspect(result)}"

    assert {_, {:fatal, _}} = result
  end

  test "reservation identity and generation survive FlatBuffer roundtrip", %{initial: initial} do
    reserved =
      Map.merge(initial, %{
        protocol_version: 1,
        recovery_generation: 8,
        recovery_id: "request-8",
        publication_id: "completed-7"
      })

    assert {:ok, decoded} = reserved |> ClusterBootstrap.to_binary() |> ClusterBootstrap.read()
    assert decoded[:recovery_generation] == 8
    assert decoded[:recovery_id] == "request-8"
    assert decoded[:protocol_version] == 1
    assert decoded[:publication_id] == "completed-7"
    assert decoded.epoch == 7
    assert Enum.map(decoded.logs, & &1.id) == ["committed-7"]
  end

  defmodule LostReplyBackend do
    @moduledoc false
    def get_with_version(config, key), do: LocalFilesystem.get_with_version(config, key)
    def get(config, key), do: LocalFilesystem.get(config, key)

    def put_if_version_matches(config, key, token, bytes, opts) do
      :ok = LocalFilesystem.put_if_version_matches(config, key, token, bytes, opts)
      {:error, :timeout}
    end
  end

  test "a successful CAS with lost reply verifies exact publication without repeating system commit", fixture do
    {_, options} = fixture.backend
    backend = {LostReplyBackend, options}
    Application.put_env(:bedrock, Cluster, object_storage: backend)
    context = context(%{fixture | backend: backend})
    owner = self()

    context = %{
      context
      | commit_transaction_fn: fn _, _, _ ->
          send(owner, :system_commit)
          {:ok, 1, 0}
        end
    }

    result = PersistencePhase.execute(attempt(8), context)
    assert_received :system_commit
    assert {:ok, bytes} = ObjectStorage.get(fixture.backend, "bootstrap")
    assert {:ok, %{epoch: 8}} = ClusterBootstrap.read(bytes)
    assert {_, :completed} = result
    refute_received :system_commit
  end

  defmodule FutureBootstrap do
    @moduledoc false
    use Flatbuffer, file: "test/support/schemas/future_cluster_bootstrap.fbs"
  end

  test "discovery fails closed on an unsupported bootstrap protocol", %{backend: backend, initial: initial} do
    future =
      Map.merge(initial, %{
        protocol_version: 2,
        recovery_generation: 8,
        recovery_id: "future-recovery",
        publication_id: "prior-publication"
      })

    :ok = ObjectStorage.put(backend, "bootstrap", FutureBootstrap.to_binary(future))

    assert {:error, {:unsupported_bootstrap_protocol, 2}} =
             Discovery.discover(backend, "bootstrap", node())
  end

  defp attempt(epoch) do
    recovery_attempt(%{
      cluster: Cluster,
      epoch: epoch,
      proxies: [self()],
      transaction_system_layout: %{
        epoch: epoch,
        logs: %{"attempt-#{epoch}" => []},
        proxies: [self()],
        sequencer: self(),
        resolvers: []
      }
    })
  end

  defp context(%{backend: backend, reservation: reservation}) do
    recovery_context(%{
      cluster_config: Config.new([node()]),
      bootstrap_reservation: %{reservation | backend: backend},
      commit_transaction_fn: fn _, _, _ -> {:ok, 1, 0} end
    })
  end
end
