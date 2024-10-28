defmodule Bedrock.ControlPlane.Config.ServiceDescriptor do
  @type id :: String.t()
  @type kind :: :log | :storage
  @type otp_name :: atom()
  @type status :: {:up, pid()} | :unknown | :down

  @type t :: %__MODULE__{
          id: id(),
          kind: kind(),
          last_seen: {otp_name(), node()} | nil,
          status: status()
        }
  defstruct [:id, :kind, :last_seen, :status]

  @spec new(id(), kind(), status()) :: t()
  def new(id, kind, status \\ :unknown), do: %__MODULE__{id: id, kind: kind, status: status}

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
  @spec upsert([t()], t()) :: [t()]
  def upsert([], n), do: [n]
  def upsert([%{id: id} | t], %{id: id} = n), do: [n | t]
  def upsert([h | t], n), do: [h | upsert(t, n)]

  @spec find_by_id([t()], id()) :: t() | nil
  def find_by_id(l, id), do: l |> Enum.find(&(&1.id == id))

  @spec remove_by_id([t()], id()) :: [t()]
  def remove_by_id(l, id), do: l |> Enum.reject(&(&1.id == id))

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

  @spec is_log?(t()) :: boolean()
  def is_log?(t), do: t.kind == :log

  @spec is_storage?(t()) :: boolean()
  def is_storage?(t), do: t.kind == :storage
end
