defmodule Bedrock.ClusterBootstrap.Discovery do
  @moduledoc """
  Discovers or creates ClusterBootstrap at node startup.

  ## Boot Sequence

  1. Try to read ClusterBootstrap from ObjectStorage
  2. If not found, attempt conditional write (race for first boot)
  3. If write succeeds → become coordinator
  4. If write fails (someone beat us) → re-read and check coordinator list

  ## First Boot

  On first boot (no ClusterBootstrap exists), nodes race to write an initial
  bootstrap using conditional PUT. The winner becomes the sole coordinator
  of a single-node cluster. Losers re-read and join as workers (or coordinators
  if the winner included them).
  """

  alias Bedrock.Internal.Id
  alias Bedrock.ObjectStorage
  alias Bedrock.SystemKeys.ClusterBootstrap

  @type bootstrap :: %{
          cluster_id: String.t(),
          epoch: pos_integer(),
          logs: list(),
          coordinators: [%{node: String.t()}]
        }

  @type discovery_result ::
          {:ok, :coordinator, bootstrap()}
          | {:ok, :worker, bootstrap()}

  @doc """
  Discovers the cluster bootstrap, creating one if this is first boot.

  Returns `{:ok, :coordinator, bootstrap}` if this node should run as a coordinator,
  or `{:ok, :worker, bootstrap}` if this node should run as a worker.
  """
  @spec discover(ObjectStorage.backend(), String.t(), node()) :: discovery_result()
  def discover(backend, bootstrap_key, self_node) do
    case ObjectStorage.get(backend, bootstrap_key) do
      {:ok, data} ->
        handle_existing_bootstrap(data, self_node)

      {:error, :not_found} ->
        handle_first_boot(backend, bootstrap_key, self_node)
    end
  end

  @doc """
  Creates an initial ClusterBootstrap for first boot.

  The bootstrap has:
  - A randomly generated cluster_id
  - epoch set to 1
  - Empty logs list
  - The given node as the sole coordinator
  """
  @spec create_initial(node()) :: bootstrap()
  def create_initial(coordinator_node) do
    %{
      cluster_id: Id.random(),
      epoch: 1,
      logs: [],
      coordinators: [%{node: Atom.to_string(coordinator_node)}]
    }
  end

  # Private functions

  defp handle_existing_bootstrap(data, self_node) do
    {:ok, bootstrap} = ClusterBootstrap.read(data)
    role = determine_role(bootstrap, self_node)
    {:ok, role, bootstrap}
  end

  defp handle_first_boot(backend, bootstrap_key, self_node) do
    initial = create_initial(self_node)
    data = ClusterBootstrap.to_binary(initial)

    case ObjectStorage.put_if_not_exists(backend, bootstrap_key, data) do
      :ok ->
        # We won the race - we're the coordinator
        {:ok, :coordinator, initial}

      {:error, :already_exists} ->
        # Someone beat us - re-read and check our role
        {:ok, data} = ObjectStorage.get(backend, bootstrap_key)
        handle_existing_bootstrap(data, self_node)
    end
  end

  defp determine_role(bootstrap, self_node) do
    self_node_str = Atom.to_string(self_node)

    is_coordinator =
      Enum.any?(bootstrap.coordinators, fn coord ->
        coord.node == self_node_str
      end)

    if is_coordinator, do: :coordinator, else: :worker
  end
end
