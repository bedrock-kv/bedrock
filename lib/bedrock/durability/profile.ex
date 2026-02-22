defmodule Bedrock.Durability.Profile do
  @moduledoc """
  Durability profile evaluation model.

  The profile currently verifies:

  - desired replication factor
  - desired logs
  - persistent coordinator path
  - persistent log path
  - persistent materializer path
  """

  @type status :: :ok | :failed
  @type check_name ::
          :desired_replication_factor
          | :desired_logs
          | :coordinator_path
          | :log_path
          | :materializer_path
  @type reason ::
          :desired_replication_factor_too_low
          | :desired_logs_too_low
          | :missing_coordinator_path
          | :missing_log_path
          | :missing_materializer_path
  @type check_result :: %{
          status: status(),
          expected: term(),
          actual: term(),
          reason: reason() | nil
        }
  @type t :: %__MODULE__{
          status: status(),
          checks: %{check_name() => check_result()},
          reasons: [reason()],
          evaluated_at_ms: non_neg_integer()
        }

  @default_min_replication_factor 3
  @default_min_logs 3

  defstruct status: :ok,
            checks: %{},
            reasons: [],
            evaluated_at_ms: 0

  @doc """
  Evaluates durability requirements against a cluster module or config.
  """
  @spec evaluate(module() | keyword() | %{optional(:node_config) => keyword(), optional(:cluster_config) => map()}) ::
          t()
  def evaluate(target) do
    %{node_config: node_config, cluster_config: cluster_config} = resolve_target(target)

    desired_replication_factor = desired_parameter(cluster_config, :desired_replication_factor, 1)
    desired_logs = desired_parameter(cluster_config, :desired_logs, 1)

    coordinator_path = role_path(node_config, :coordinator)
    log_path = role_path(node_config, :log)
    materializer_path = role_path(node_config, :materializer)

    checks = %{
      desired_replication_factor:
        minimum_check(
          desired_replication_factor,
          @default_min_replication_factor,
          :desired_replication_factor_too_low
        ),
      desired_logs: minimum_check(desired_logs, @default_min_logs, :desired_logs_too_low),
      coordinator_path: presence_check(coordinator_path, :missing_coordinator_path),
      log_path: presence_check(log_path, :missing_log_path),
      materializer_path: presence_check(materializer_path, :missing_materializer_path)
    }

    reasons =
      Enum.flat_map(checks, fn
        {_check, %{status: :failed, reason: reason}} when is_atom(reason) -> [reason]
        _ -> []
      end)

    %__MODULE__{
      status: if(reasons == [], do: :ok, else: :failed),
      checks: checks,
      reasons: reasons,
      evaluated_at_ms: System.system_time(:millisecond)
    }
  end

  defp resolve_target(target) when is_atom(target) do
    node_config =
      if function_exported?(target, :node_config, 0) do
        target.node_config()
      else
        []
      end

    cluster_config =
      if function_exported?(target, :fetch_config, 0) do
        case target.fetch_config() do
          {:ok, config} -> config
          _ -> nil
        end
      end

    %{node_config: node_config, cluster_config: cluster_config}
  end

  defp resolve_target(target) when is_list(target) do
    %{node_config: target, cluster_config: nil}
  end

  defp resolve_target(%{} = target) do
    %{
      node_config: Map.get(target, :node_config, []),
      cluster_config: Map.get(target, :cluster_config)
    }
  end

  defp desired_parameter(cluster_config, key, default) do
    case get_in(cluster_config || %{}, [:parameters, key]) do
      value when is_integer(value) and value > 0 -> value
      _ -> default
    end
  end

  defp role_path(node_config, :coordinator) do
    node_config
    |> Keyword.get(:coordinator, [])
    |> Keyword.get(:path)
  end

  defp role_path(node_config, :log) do
    node_config
    |> Keyword.get(:log, [])
    |> Keyword.get(:path) ||
      worker_path(node_config)
  end

  defp role_path(node_config, :materializer) do
    node_config
    |> Keyword.get(:materializer, [])
    |> Keyword.get(:path) ||
      node_config
      |> Keyword.get(:storage, [])
      |> Keyword.get(:path) ||
      worker_path(node_config)
  end

  defp worker_path(node_config) do
    node_config
    |> Keyword.get(:worker, [])
    |> Keyword.get(:path)
  end

  defp minimum_check(actual, min_expected, failure_reason) when is_integer(actual) and is_integer(min_expected) do
    if actual >= min_expected do
      %{status: :ok, expected: min_expected, actual: actual, reason: nil}
    else
      %{status: :failed, expected: min_expected, actual: actual, reason: failure_reason}
    end
  end

  defp presence_check(value, failure_reason) do
    if is_binary(value) and value != "" do
      %{status: :ok, expected: :present, actual: :present, reason: nil}
    else
      %{status: :failed, expected: :present, actual: :missing, reason: failure_reason}
    end
  end
end
