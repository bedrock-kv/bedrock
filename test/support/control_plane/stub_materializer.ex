defmodule Bedrock.Test.ControlPlane.StubMaterializer do
  @moduledoc """
  A stub materializer GenServer that implements the real read API call
  shapes handled by the olivine/basalt materializer servers:

    * `{:get, key | KeySelector.t(), version, opts}`
    * `{:get_range, start_key | KeySelector.t(), end_key | KeySelector.t(), version, opts}`

  Serves from a static key-value map, ignoring versions.
  """
  use GenServer

  alias Bedrock.KeySelector

  @spec start_link(%{Bedrock.key() => Bedrock.value()}) :: GenServer.on_start()
  def start_link(kvs), do: GenServer.start_link(__MODULE__, kvs)

  @impl true
  def init(kvs), do: {:ok, kvs}

  @impl true
  def handle_call({:get, %KeySelector{key: key}, _version, _opts}, _from, kvs) do
    case Map.fetch(kvs, key) do
      {:ok, value} -> {:reply, {:ok, {key, value}}, kvs}
      :error -> {:reply, {:ok, nil}, kvs}
    end
  end

  def handle_call({:get, key, _version, _opts}, _from, kvs) when is_binary(key),
    do: {:reply, {:ok, Map.get(kvs, key)}, kvs}

  def handle_call({:get_range, start_key, end_key, _version, _opts}, _from, kvs)
      when is_binary(start_key) and is_binary(end_key) do
    results =
      kvs
      |> Enum.filter(fn {key, _value} -> key >= start_key and key < end_key end)
      |> Enum.sort()

    {:reply, {:ok, {results, false}}, kvs}
  end
end
