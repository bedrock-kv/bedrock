defmodule Bedrock.Test.ControlPlane.StubMaterializer do
  @moduledoc """
  A stub materializer GenServer that implements the real read API call
  shapes handled by the olivine/basalt materializer servers:

    * `{:get, key | KeySelector.t(), version, opts}`
    * `{:get_range, start_key | KeySelector.t(), end_key | KeySelector.t(), version, opts}`

  Serves from a static key-value map, ignoring versions.

  Also speaks the recovery worker protocol (`{:lock_for_recovery, epoch}`
  and `{:unlock_after_recovery, durable_version, tsl}`) so recruitment
  tests can stub only the foreman worker-creation boundary while keeping
  the real epoch lock/unlock calls. Lock/unlock activity is reported to an
  optional observer pid as `{:stub_materializer, event}` messages.
  """
  use GenServer

  alias Bedrock.DataPlane.Version
  alias Bedrock.KeySelector

  @spec start_link(%{Bedrock.key() => Bedrock.value()}, opts :: keyword()) :: GenServer.on_start()
  def start_link(kvs, opts \\ []) do
    {observer, opts} = Keyword.pop(opts, :observer)
    GenServer.start_link(__MODULE__, {kvs, observer}, opts)
  end

  @impl true
  def init({kvs, observer}), do: {:ok, %{kvs: kvs, observer: observer}}
  def init(kvs), do: {:ok, %{kvs: kvs, observer: nil}}

  @impl true
  def handle_call({:get, %KeySelector{key: key}, _version, _opts}, _from, %{kvs: kvs} = state) do
    case Map.fetch(kvs, key) do
      {:ok, value} -> {:reply, {:ok, {key, value}}, state}
      :error -> {:reply, {:ok, nil}, state}
    end
  end

  def handle_call({:get, key, _version, _opts}, _from, %{kvs: kvs} = state) when is_binary(key),
    do: {:reply, {:ok, Map.get(kvs, key)}, state}

  def handle_call({:get_range, start_key, end_key, _version, _opts}, _from, %{kvs: kvs} = state)
      when is_binary(start_key) and is_binary(end_key) do
    results =
      kvs
      |> Enum.filter(fn {key, _value} -> key >= start_key and key < end_key end)
      |> Enum.sort()

    {:reply, {:ok, {results, false}}, state}
  end

  def handle_call({:lock_for_recovery, epoch}, _from, state) do
    notify(state, {:locked_for_recovery, self(), epoch})

    recovery_info = %{
      kind: :materializer,
      durable_version: Version.zero(),
      oldest_durable_version: Version.zero()
    }

    {:reply, {:ok, self(), recovery_info}, state}
  end

  def handle_call({:unlock_after_recovery, durable_version, tsl}, _from, state) do
    notify(state, {:unlocked_after_recovery, self(), durable_version, tsl})
    {:reply, :ok, state}
  end

  defp notify(%{observer: nil}, _event), do: :ok
  defp notify(%{observer: observer}, event), do: send(observer, {:stub_materializer, event})
end
