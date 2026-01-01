defmodule Bedrock.ControlPlane.Director.ChangingParameters do
  @moduledoc false

  import Bedrock.ControlPlane.Config.Changes,
    only: [put_parameters: 2]

  import Bedrock.ControlPlane.Config.Parameters,
    only: [put_desired_replication_factor: 2]

  import Bedrock.ControlPlane.Director.State.Changes,
    only: [update_config: 2]

  alias Bedrock.ControlPlane.Config.Parameters
  alias Bedrock.ControlPlane.Director.State

  @type parameter_name ::
          :ping_rate_in_hz
          | :retransmission_rate_in_hz
          | :replication_factor
          | :desired_coordinators
          | :desired_logs
          | :desired_read_version_proxies
          | :desired_commit_proxies
          | :desired_transaction_resolvers
          | :transaction_window_in_ms

  @type parameter_value :: pos_integer()
  @type parameter_change :: {parameter_name(), parameter_value()}
  @type parameter_changes :: [parameter_change()]
  @type cluster_state :: :uninitialized | :initializing | :running | :stopped
  @type invalid_parameters :: [{parameter_name(), parameter_value()}]

  @spec settable_parameters_for_state(cluster_state()) :: [parameter_name()]
  def settable_parameters_for_state(:uninitialized),
    do: [
      :ping_rate_in_hz,
      :retransmission_rate_in_hz,
      :replication_factor,
      :desired_coordinators,
      :desired_logs,
      :desired_read_version_proxies,
      :desired_commit_proxies,
      :desired_transaction_resolvers,
      :transaction_window_in_ms
    ]

  def settable_parameters_for_state(_), do: []

  @spec try_to_set_parameters_in_config(
          t :: State.t(),
          list :: [
            ping_rate_in_hz: integer(),
            retransmission_rate_in_hz: integer(),
            replication_factor: integer(),
            desired_coordinators: integer(),
            desired_logs: integer(),
            desired_read_version_proxies: integer(),
            desired_commit_proxies: integer(),
            desired_transaction_resolvers: integer(),
            transaction_window_in_ms: integer()
          ]
        ) ::
          {:ok, State.t()}
          | {:error, :invalid_parameters_for_state, invalid_parameters()}
          | {:error, :invalid_value}
  def try_to_set_parameters_in_config(t, list) do
    with :ok <- validate_settable_parameters_for_state(list, t.config.state),
         {:ok, updated_parameters} <- try_to_set_parameters(t.config.parameters, list) do
      t =
        update_config(t, fn config ->
          put_parameters(config, updated_parameters)
        end)

      {:ok, t}
    end
  end

  @spec validate_settable_parameters_for_state(
          [
            ping_rate_in_hz: integer(),
            retransmission_rate_in_hz: integer(),
            replication_factor: integer(),
            desired_coordinators: integer(),
            desired_logs: integer(),
            desired_read_version_proxies: integer(),
            desired_commit_proxies: integer(),
            desired_transaction_resolvers: integer(),
            transaction_window_in_ms: integer()
          ],
          cluster_state()
        ) ::
          :ok | {:error, :invalid_parameters_for_state, invalid_parameters()}
  def validate_settable_parameters_for_state(parameters, state) do
    parameters
    |> Keyword.drop(settable_parameters_for_state(state))
    |> case do
      [] -> :ok
      unsupported_parameters -> {:error, :invalid_parameters_for_state, unsupported_parameters}
    end
  end

  @spec try_to_set_parameters(Parameters.t(), parameter_changes()) ::
          {:ok, Parameters.t()} | {:error, :invalid_value}
  def try_to_set_parameters(parameters, list) do
    list
    |> Enum.reduce(parameters, fn
      {parameter_name, value}, parameters ->
        parameters
        |> try_to_set_parameter(parameter_name, value)
        |> case do
          {:ok, p} -> {:cont, p}
          {:error, _reason} = error -> {:halt, error}
        end
    end)
    |> case do
      {:error, _reason} = error -> error
      parameters -> {:ok, parameters}
    end
  end

  @spec try_to_set_parameter(Parameters.t(), parameter_name(), parameter_value()) ::
          {:ok, Parameters.t()} | {:error, :invalid_value}
  def try_to_set_parameter(parameters, :replication_factor, n), do: {:ok, put_desired_replication_factor(parameters, n)}

  def try_to_set_parameter(_, _, _), do: {:error, :invalid_value}
end
