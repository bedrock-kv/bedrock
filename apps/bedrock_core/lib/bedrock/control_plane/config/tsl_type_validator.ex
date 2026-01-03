defmodule Bedrock.ControlPlane.Config.TSLTypeValidator do
  @moduledoc """
  TSL (Transaction System Layout) type safety validation to prevent data corruption.

  This module provides comprehensive type checking for TSL fields to prevent issues
  like integer-to-binary version conversion errors that can cause MVCC lookup failures.
  The critical distinction is between:
  - Log ranges: should contain integers (not binary versions)
  - Version fields: should contain Version.t() binary versions

  Provides both defensive validation (returns errors) and assertive validation (raises).
  """

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.Version

  @doc """
  Validates TSL type safety defensively, returning error tuples.

  Use this for validating old/recovered data where corruption should be handled gracefully.
  """
  @spec validate_type_safety(TransactionSystemLayout.t()) :: :ok | {:error, term()}
  def validate_type_safety(%{} = tsl) do
    with :ok <- validate_logs(Map.get(tsl, :logs)),
         :ok <- validate_storage_teams(Map.get(tsl, :storage_teams)) do
      validate_resolvers(Map.get(tsl, :resolvers))
    end
  end

  @doc """
  Validates TSL type safety assertively, raising on errors.

  Use this for validating new data where type mismatches indicate programmer errors.
  """
  @spec assert_type_safety!(TransactionSystemLayout.t()) :: TransactionSystemLayout.t()
  def assert_type_safety!(%{} = tsl) do
    case validate_type_safety(tsl) do
      :ok ->
        tsl

      {:error, reason} ->
        raise ArgumentError, """
        TSL type safety assertion failed: #{inspect(reason)}

        This indicates a programmer error - new TSL data should have correct types.
        TransactionSystemLayout: #{inspect(tsl, limit: :infinity)}
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

  # Log ranges should be [start_int, end_int] where both are integers
  defp validate_log_ranges([start_range, end_range])
       when is_integer(start_range) and is_integer(end_range) and start_range <= end_range do
    # Critical check: ensure these are integers, NOT Version.t() binaries
    cond do
      Version.valid?(start_range) ->
        {:error,
         {:version_in_log_range, "log range start is Version.t() binary, should be integer: #{inspect(start_range)}"}}

      Version.valid?(end_range) ->
        {:error,
         {:version_in_log_range, "log range end is Version.t() binary, should be integer: #{inspect(end_range)}"}}

      true ->
        :ok
    end
  end

  defp validate_log_ranges(ranges) do
    {:error, {:invalid_log_ranges, "expected [start_int, end_int], got: #{inspect(ranges)}"}}
  end

  # Validate storage_teams: [StorageTeamDescriptor.t()]
  defp validate_storage_teams(nil), do: :ok
  defp validate_storage_teams([]), do: :ok

  defp validate_storage_teams(storage_teams) when is_list(storage_teams) do
    Enum.reduce_while(storage_teams, :ok, fn storage_team, :ok ->
      case validate_storage_team(storage_team) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:invalid_storage_teams, reason}}}
      end
    end)
  end

  defp validate_storage_teams(storage_teams) do
    {:error, {:invalid_storage_teams_structure, "storage_teams must be a list, got: #{inspect(storage_teams)}"}}
  end

  defp validate_storage_team(%{tag: tag, key_range: key_range, storage_ids: storage_ids})
       when is_integer(tag) and tag >= 0 do
    with :ok <- validate_key_range(key_range) do
      validate_storage_ids(storage_ids)
    end
  end

  defp validate_storage_team(storage_team) do
    {:error, {:invalid_storage_team_structure, "invalid storage team format: #{inspect(storage_team)}"}}
  end

  defp validate_key_range({start_key, end_key}) when is_binary(start_key) do
    case end_key do
      :end -> :ok
      <<0xFF, 0xFF>> -> :ok
      end_key when is_binary(end_key) -> :ok
      _ -> {:error, {:invalid_key_range, "invalid end key: #{inspect(end_key)}"}}
    end
  end

  defp validate_key_range(key_range) do
    {:error, {:invalid_key_range, "invalid key range format: #{inspect(key_range)}"}}
  end

  defp validate_storage_ids(storage_ids) when is_list(storage_ids) do
    Enum.reduce_while(storage_ids, :ok, fn storage_id, :ok ->
      case validate_storage_id(storage_id) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp validate_storage_ids(storage_ids) do
    {:error, {:invalid_storage_ids, "storage_ids must be a list, got: #{inspect(storage_ids)}"}}
  end

  defp validate_storage_id(storage_id) when is_binary(storage_id), do: :ok
  defp validate_storage_id({:vacancy, tag}) when is_integer(tag) and tag > 0, do: :ok

  defp validate_storage_id(storage_id) do
    {:error, {:invalid_storage_id, "expected string or {:vacancy, pos_integer}, got: #{inspect(storage_id)}"}}
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
