defmodule Bedrock.ControlPlane.Coordinator do
  @moduledoc """
  The Coordinator module is responsible for managing the state of the cluster.
  """
  alias Bedrock.ControlPlane.Director
  alias Bedrock.ControlPlane.Config

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: GenServer.name()
  @typep timeout_in_ms :: Bedrock.timeout_in_ms()

  def config_key, do: :coordinator

  @spec fetch_director(coordinator :: ref(), timeout_in_ms()) ::
          {:ok, Director.ref()} | {:error, :unavailable | :timeout}
  def fetch_director(coordinator, timeout \\ 5_000),
    do: coordinator |> call(:fetch_director, timeout)

  @spec fetch_config(coordinator :: ref(), timeout_in_ms()) ::
          {:ok, Config.t()} | {:error, :unavailable | :timeout}
  def fetch_config(coordinator, timeout \\ 5_000),
    do: coordinator |> call(:fetch_config, timeout)

  @spec write_config(coordinator :: ref(), config :: Config.t(), timeout()) ::
          :ok | {:error, :unavailable}
  def write_config(coordinator, config, timeout \\ 5_000),
    do: coordinator |> call({:write_config, config}, timeout)
end
