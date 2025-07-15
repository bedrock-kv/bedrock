defmodule Bedrock.ControlPlane.Coordinator do
  @moduledoc """
  The Coordinator module is responsible for managing the state of the cluster.
  """
  alias Bedrock.ControlPlane.Director
  alias Bedrock.ControlPlane.Config

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: GenServer.name()
  @typep timeout_in_ms :: Bedrock.timeout_in_ms()

  @spec config_key() :: atom()
  def config_key, do: :coordinator

  @spec fetch_director_and_epoch(
          coordinator_ref :: ref(),
          opts :: [
            timeout_in_ms: Bedrock.timeout_in_ms()
          ]
        ) ::
          {:ok, {director_ref :: Director.ref(), current_epoch :: Bedrock.epoch()}}
          | {:error, :unavailable | :timeout}
  def fetch_director_and_epoch(coordinator, opts \\ []),
    do: coordinator |> call(:fetch_director_and_epoch, opts[:timeout_in_ms] || :infinity)

  @spec fetch_config(coordinator_ref :: ref(), timeout_ms :: timeout_in_ms()) ::
          {:ok, config :: Config.t()} | {:error, :unavailable | :timeout}
  def fetch_config(coordinator, timeout \\ 5_000),
    do: coordinator |> call(:fetch_config, timeout)

  @spec write_config(coordinator_ref :: ref(), config :: Config.t(), timeout_ms :: timeout()) ::
          :ok | {:error, :unavailable}
  def write_config(coordinator, config, timeout \\ 5_000),
    do: coordinator |> call({:write_config, config}, timeout)
end
