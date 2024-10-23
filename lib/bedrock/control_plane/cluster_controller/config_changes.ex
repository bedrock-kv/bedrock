defmodule Bedrock.ControlPlane.ClusterController.ConfigChanges do
  alias Bedrock.ControlPlane.ClusterController.State
  alias Bedrock.ControlPlane.Config.Parameters

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

  @spec try_to_set_parameters_in_config(t :: State.t(), list :: keyword()) ::
          {:ok, State.t()}
          | {:error, :invalid_parameters_for_state, [atom()]}
          | {:error, :invalid_value}
  def try_to_set_parameters_in_config(t, list) do
    with :ok <- validate_settable_parameters_for_state(list, t.config.state),
         {:ok, updated_parameters} <- try_to_set_parameters(t.config.parameters, list) do
      {:ok, put_in(t.config.parameters, updated_parameters)}
    end
  end

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

  def try_to_set_parameter(parameters, :replication_factor, n),
    do: {:ok, put_in(parameters.replication_factor, n)}

  def try_to_set_parameter(_parameters, _parameter_name, _value), do: {:error, :invalid_value}
end
