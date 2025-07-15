defmodule Bedrock.ControlPlane.Director.ChangingParameters do
  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.ControlPlane.Config.Parameters

  import Bedrock.ControlPlane.Director.State.Changes,
    only: [update_config: 2]

  import Bedrock.ControlPlane.Config.Changes,
    only: [put_parameters: 2]

  import Bedrock.ControlPlane.Config.Parameters,
    only: [put_desired_replication_factor: 2]

  @spec settable_parameters_for_state(atom()) :: [atom()]
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
          | {:error, :invalid_parameters_for_state, [{atom(), any()}]}
          | {:error, :invalid_value}
  def try_to_set_parameters_in_config(t, list) do
    with :ok <- validate_settable_parameters_for_state(list, t.config.state),
         {:ok, updated_parameters} <- try_to_set_parameters(t.config.parameters, list) do
      t =
        t
        |> update_config(fn config ->
          config
          |> put_parameters(updated_parameters)
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
          atom()
        ) ::
          :ok | {:error, :invalid_parameters_for_state, keyword()}
  def validate_settable_parameters_for_state(parameters, state) do
    parameters
    |> Keyword.drop(settable_parameters_for_state(state))
    |> case do
      [] -> :ok
      unsupported_parameters -> {:error, :invalid_parameters_for_state, unsupported_parameters}
    end
  end

  @spec try_to_set_parameters(Parameters.t(), list :: keyword()) ::
          {:ok, Parameters.t()} | {:error, :invalid_value}
  def try_to_set_parameters(parameters, list) do
    Enum.reduce(list, parameters, fn
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

  @spec try_to_set_parameter(Parameters.t(), atom(), any()) ::
          {:ok, Parameters.t()} | {:error, :invalid_value}
  def try_to_set_parameter(parameters, :replication_factor, n),
    do: {:ok, put_desired_replication_factor(parameters, n)}

  def try_to_set_parameter(_, _, _), do: {:error, :invalid_value}
end
