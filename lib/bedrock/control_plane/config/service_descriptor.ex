defmodule Bedrock.ControlPlane.Config.ServiceDescriptor do
  @type id :: String.t()
  @type kind :: :log | :storage
  @type otp_name :: atom()
  @type status :: :unknown | :down | {:up, pid(), otp_name(), node()}

  @type t :: %__MODULE__{
          id: id(),
          kind: kind(),
          status: status()
        }
  defstruct [:id, :kind, :status]

  @spec new(id(), kind(), status()) :: t()
  def new(id, kind, status \\ :unknown), do: %__MODULE__{id: id, kind: kind, status: status}

  @spec up(t(), pid(), otp_name(), node()) :: t()
  def up(t, pid, otp_name, node), do: put_in(t.status, {:up, pid, otp_name, node})

  @spec down(t()) :: t()
  def down(t), do: put_in(t.status, :down)

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
  def node_down(%{status: {:up, _pid, _otp_name, node}} = t, node), do: put_in(t.status, :down)
  def node_down(t, _node), do: t
end
