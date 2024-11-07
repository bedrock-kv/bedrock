defmodule Bedrock.ControlPlane.Config.ServiceDescriptor do
  alias Bedrock.Service.Worker

  @type id :: String.t()
  @type kind :: :log | :storage
  @type otp_name :: atom()
  @type status :: {:up, pid()} | :unknown | :down

  @type t :: %{
          id: id(),
          kind: kind(),
          last_seen: {otp_name(), node()} | nil,
          status: status()
        }

  @spec service_descriptor(id(), kind(), status()) :: t()
  def service_descriptor(id, kind, status \\ :unknown),
    do: %{
      id: id,
      kind: kind,
      status: status,
      last_seen: nil
    }

  @spec up(t(), pid()) :: t()
  def up(t, pid), do: %{t | status: {:up, pid}}

  @spec up(t(), pid(), otp_name(), node()) :: t()
  def up(t, pid, otp_name, node), do: %{t | status: {:up, pid}, last_seen: {otp_name, node}}

  @spec down(t()) :: t()
  def down(t), do: t |> put_status(:down)

  @doc """
  Inserts a service descriptor into a list of service descriptors, replacing
  any existing service descriptor with the same id.
  """
  @spec upsert(%{Worker.id() => ServiceDescriptor.t()}, t()) ::
          %{Worker.id() => ServiceDescriptor.t()}
  def upsert(t, n), do: Map.put(t, n.id, n)

  @spec find_by_id(%{Worker.id() => t()}, id()) :: t() | nil
  def find_by_id(l, id), do: Map.get(l, id)

  @spec remove_by_id(%{Worker.id() => t()}, id()) :: %{Worker.id() => t()}
  def remove_by_id(l, id), do: Map.delete(l, id)

  @spec find_pid_by_id(%{Worker.id() => t()}, id()) :: pid() | nil
  def find_pid_by_id(l, id) do
    case Map.get(l, id) do
      %{status: {:up, pid}} -> pid
      _ -> nil
    end
  end

  @doc """
  Changes the status of a service descriptor to `:down` if it is currently `:up`
  and running on the given node.
  """
  @spec node_down(t(), node()) :: t()
  def node_down(%{last_seen: {_otp_name, node}, status: {:up, _pid}} = t, node),
    do: t |> put_status(:down)

  def node_down(t, _node), do: t

  @spec put_status(t(), status()) :: t()
  def put_status(t, status), do: %{t | status: status}

  @spec log_worker?(t()) :: boolean()
  def log_worker?(t), do: t.kind == :log

  @spec storage_worker?(t()) :: boolean()
  def storage_worker?(t), do: t.kind == :storage
end
