defmodule Bedrock.ControlPlane.Config.TypeSafetyValidator do
  @moduledoc """
  Type-checks the two fields a corrupt value can smuggle a wrong type
  through: `logs` and `resolvers`.

  It checks exactly those two, and it is deliberately not named for one
  record, because both of the config records that carry them go through
  it:

    * `CoreState` — the DURABLE record, read back off object storage and
      possibly written by a previous version of this software. Checked
      defensively by `validate_type_safety/1`, from
      `CoreStateValidationPhase`; it carries `logs` and no `resolvers`,
      so the resolver half is vacuous there.
    * `TransactionSystemLayout` — the TRANSIENT broadcast this recovery
      just built. Asserted by `assert_type_safety!/1`, from
      `TopologyPhase`; a wrong type in a layout recovery assembled itself
      is a programmer error, so it raises.

  The corruption it exists to catch is integer-to-binary version
  conversion, which causes MVCC lookup failures far from its cause. The
  critical distinction:
  - Log ranges: should contain integers (not binary versions)
  - Version fields: should contain Version.t() binary versions
  """

  alias Bedrock.ControlPlane.Config.CoreState
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @doc """
  Validates type safety defensively, returning error tuples.

  Use this for validating old/recovered data where corruption should be handled gracefully.
  """
  @spec validate_type_safety(CoreState.t() | TransactionSystemLayout.t()) :: :ok | {:error, term()}
  def validate_type_safety(%{} = record) do
    with :ok <- validate_logs(Map.get(record, :logs)) do
      validate_resolvers(Map.get(record, :resolvers))
    end
  end

  @doc """
  Validates type safety assertively, raising on errors.

  Use this for validating new data where type mismatches indicate programmer errors.
  """
  @spec assert_type_safety!(TransactionSystemLayout.t()) :: TransactionSystemLayout.t()
  def assert_type_safety!(%{} = transaction_system_layout) do
    case validate_type_safety(transaction_system_layout) do
      :ok ->
        transaction_system_layout

      {:error, reason} ->
        raise ArgumentError, """
        Type safety assertion failed: #{inspect(reason)}

        This indicates a programmer error - a freshly built layout should have correct types.
        TransactionSystemLayout: #{inspect(transaction_system_layout, limit: :infinity)}
        """
    end
  end

  # Validate logs field: %{log_vacancy() => [integer(), integer()]}
  # Log ranges must be integers, NOT binary versions
  defp validate_logs(nil), do: :ok
  defp validate_logs(logs) when is_map(logs) and map_size(logs) == 0, do: :ok

  defp validate_logs(logs) when is_map(logs) do
    Enum.reduce_while(logs, :ok, fn {log_id, log_ranges}, :ok ->
      case validate_log_entry(log_id, log_ranges) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:invalid_logs, log_id, reason}}}
      end
    end)
  end

  defp validate_logs(logs) do
    {:error, {:invalid_logs_structure, "logs must be a map, got: #{inspect(logs)}"}}
  end

  defp validate_log_entry(log_id, log_ranges) do
    with :ok <- validate_log_id(log_id) do
      validate_log_ranges(log_ranges)
    end
  end

  # Log ID should be either a string (actual log ID) or {:vacancy, integer} tuple
  defp validate_log_id(log_id) when is_binary(log_id), do: :ok
  defp validate_log_id({:vacancy, tag}) when is_integer(tag) and tag > 0, do: :ok

  defp validate_log_id(log_id) do
    {:error, {:invalid_log_id, "expected string or {:vacancy, pos_integer}, got: #{inspect(log_id)}"}}
  end

  # Log ranges can be:
  # - Empty list [] for consistent hashing (shard→log mapping computed at runtime)
  # - [start_int, end_int] for legacy shard tag ranges (integers, NOT Version.t() binaries)
  defp validate_log_ranges([]), do: :ok

  defp validate_log_ranges([start_range, end_range])
       when is_integer(start_range) and is_integer(end_range) and start_range <= end_range do
    # Version.t() binaries can't match the is_integer guards; a
    # [version, version] range hits the catch-all error below.
    :ok
  end

  defp validate_log_ranges(ranges) do
    {:error, {:invalid_log_ranges, "expected [] or [start_int, end_int], got: #{inspect(ranges)}"}}
  end

  # Validate resolvers: [ResolverDescriptor.t()]
  # Should be list of {start_key, pid | {:vacancy, integer}} tuples
  defp validate_resolvers(nil), do: :ok
  defp validate_resolvers([]), do: :ok

  defp validate_resolvers(resolvers) when is_list(resolvers) do
    Enum.reduce_while(resolvers, :ok, fn resolver, :ok ->
      case validate_resolver(resolver) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:invalid_resolvers, reason}}}
      end
    end)
  end

  defp validate_resolvers(resolvers) do
    {:error, {:invalid_resolvers_structure, "resolvers must be a list, got: #{inspect(resolvers)}"}}
  end

  defp validate_resolver({start_key, resolver_ref}) when is_binary(start_key) do
    validate_resolver_ref(resolver_ref)
  end

  defp validate_resolver(%{start_key: start_key, resolver: resolver_ref}) when is_binary(start_key) do
    validate_resolver_ref(resolver_ref)
  end

  defp validate_resolver(resolver) do
    {:error, {:invalid_resolver_structure, "invalid resolver format: #{inspect(resolver)}"}}
  end

  defp validate_resolver_ref(resolver_ref) when is_pid(resolver_ref), do: :ok
  defp validate_resolver_ref({:vacancy, tag}) when is_integer(tag) and tag > 0, do: :ok

  defp validate_resolver_ref(resolver_ref) do
    {:error, {:invalid_resolver_ref, "expected pid or {:vacancy, pos_integer}, got: #{inspect(resolver_ref)}"}}
  end
end
