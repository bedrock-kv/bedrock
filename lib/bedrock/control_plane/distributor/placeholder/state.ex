defmodule Bedrock.ControlPlane.Distributor.Placeholder.State do
  @moduledoc false

  alias Bedrock.ControlPlane.Distributor.Placeholder
  alias Bedrock.Internal.WaitingList

  @unbounded_end_key <<0xFF, 0xFF>>

  @type t :: %__MODULE__{
          cluster: module(),
          distributor: pid(),
          shard_layout: %{Bedrock.key() => {Bedrock.range_tag(), Bedrock.key()}},
          hold_ms: pos_integer(),
          waiting: WaitingList.t(),
          covered: %{Bedrock.range_tag() => Placeholder.ref()},
          demanded: MapSet.t(Bedrock.range_tag()),
          expiry_timer: reference() | nil
        }
  defstruct cluster: nil,
            distributor: nil,
            shard_layout: %{},
            hold_ms: 2_000,
            waiting: %{},
            covered: %{},
            demanded: MapSet.new(),
            expiry_timer: nil

  @doc """
  Resolves the shard tag responsible for a key from the placeholder's
  copy of the shard layout (`%{end_key => {tag, start_key}}`), read from
  the committed keyspace. The end key `<<0xFF, 0xFF>>` is the unbounded
  sentinel, matching `LayoutIndex` semantics.
  """
  @spec resolve_tag(t(), Bedrock.key()) :: {:ok, Bedrock.range_tag()} | {:error, :no_shard}
  def resolve_tag(%__MODULE__{shard_layout: shard_layout}, key) do
    shard_layout
    |> Enum.find(fn {end_key, {_tag, start_key}} ->
      start_key <= key and (key < end_key or end_key == @unbounded_end_key)
    end)
    |> case do
      {_end_key, {tag, _start_key}} -> {:ok, tag}
      nil -> {:error, :no_shard}
    end
  end

  @doc """
  The parking budget for a request: the minimum of the caller-supplied
  timeout (when finite) and the configured `hold_ms` — the placeholder
  never holds a caller past its own deadline.
  """
  @spec parking_budget_ms(t(), caller_timeout :: timeout() | nil) :: non_neg_integer()
  def parking_budget_ms(%__MODULE__{hold_ms: hold_ms}, caller_timeout)
      when is_integer(caller_timeout) and caller_timeout >= 0, do: min(caller_timeout, hold_ms)

  def parking_budget_ms(%__MODULE__{hold_ms: hold_ms}, _caller_timeout), do: hold_ms
end
