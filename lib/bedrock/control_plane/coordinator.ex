defmodule Bedrock.ControlPlane.Coordinator do
  @moduledoc """
  The Coordinator module is responsible for managing the state of the cluster.
  """
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Director

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: atom() | {atom(), node()}
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

  @spec update_config(
          coordinator_ref :: ref(),
          config :: Config.t(),
          timeout_ms :: timeout_in_ms()
        ) ::
          {:ok, txn_id :: term()}
          | {:error, :unavailable}
          | {:error, :failed}
          | {:error, :not_leader}
  def update_config(coordinator, config, timeout \\ 5_000),
    do: coordinator |> call({:update_config, config}, timeout)

  @spec fetch_transaction_system_layout(
          coordinator_ref :: ref(),
          timeout_ms :: timeout_in_ms()
        ) ::
          {:ok, transaction_system_layout :: TransactionSystemLayout.t()}
          | {:error, :unavailable | :timeout}
  def fetch_transaction_system_layout(coordinator, timeout \\ 5_000),
    do: coordinator |> call(:fetch_transaction_system_layout, timeout)

  @spec update_transaction_system_layout(
          coordinator_ref :: ref(),
          transaction_system_layout :: TransactionSystemLayout.t(),
          timeout_ms :: timeout_in_ms()
        ) ::
          {:ok, txn_id :: term()}
          | {:error, :unavailable}
          | {:error, :failed}
          | {:error, :not_leader}
  def update_transaction_system_layout(coordinator, transaction_system_layout, timeout \\ 5_000),
    do:
      coordinator
      |> call({:update_transaction_system_layout, transaction_system_layout}, timeout)

  @type service_info :: {service_id :: String.t(), kind :: atom(), worker_ref :: {atom(), node()}}

  @spec register_services(
          coordinator_ref :: ref(),
          services :: [service_info()],
          timeout_ms :: timeout_in_ms()
        ) ::
          {:ok, txn_id :: term()}
          | {:error, :unavailable}
          | {:error, :failed}
          | {:error, :not_leader}
  def register_services(coordinator, services, timeout \\ 5_000),
    do: coordinator |> call({:register_services, services}, timeout)

  @spec deregister_services(
          coordinator_ref :: ref(),
          service_ids :: [String.t()],
          timeout_ms :: timeout_in_ms()
        ) ::
          {:ok, txn_id :: term()}
          | {:error, :unavailable}
          | {:error, :failed}
          | {:error, :not_leader}
  def deregister_services(coordinator, service_ids, timeout \\ 5_000),
    do: coordinator |> call({:deregister_services, service_ids}, timeout)

  @spec register_gateway(
          coordinator_ref :: ref(),
          gateway_pid :: pid(),
          services :: [service_info()],
          timeout_ms :: timeout_in_ms()
        ) ::
          {:ok, txn_id :: term()}
          | {:error, :unavailable}
          | {:error, :failed}
          | {:error, :not_leader}
  def register_gateway(coordinator, gateway_pid, services, timeout \\ 5_000),
    do: coordinator |> call({:register_gateway, gateway_pid, services}, timeout)

  @spec ping(
          coordinator_ref :: ref(),
          timeout_ms :: timeout_in_ms()
        ) ::
          {:pong, epoch :: Bedrock.epoch(), leader :: ref() | nil}
          | {:error, :timeout}
  def ping(coordinator, timeout \\ 5_000),
    do: coordinator |> call(:ping, timeout)
end
