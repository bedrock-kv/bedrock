defmodule Bedrock.DataPlane.CommitProxy.Metadata do
  @moduledoc """
  Structured system metadata maintained by the commit proxy.

  The resolver distributes version-ordered windows of committed `\\xFF`
  system-key mutations back to each commit proxy (see
  `Bedrock.DataPlane.Resolver.MetadataAccumulator`). This module reduces those
  updates into a structured, parsed view that mirrors the key families the
  director writes during recovery persistence
  (`Bedrock.ControlPlane.Director.Recovery.PersistencePhase`).

  ## Structure

  - `version` - highest commit version applied so far (nil until first update)
  - `shards` - `shard_key(end_key) -> tag` ceiling-search shard map
  - `shard_metadata` - `shard(tag) -> encoded ShardMetadata` (kept encoded;
    FlatBuffer, not `term_to_binary`)
  - `materializers` - `materializer_key(end_key) -> encoded value` (kept encoded)
  - `logs` - `layout_log(log_id) -> decoded log descriptor`
  - `services` - decoded `layout_services` map
  - `layout_id` - decoded `layout_id`
  - `cluster` - decoded fixed cluster keys (`:coordinators`, `:epoch`)
  - `policies` - decoded cluster policies (`:volunteer_nodes`)
  - `parameters` - decoded cluster parameters (`:desired_logs`, ...)
  - `recovery` - decoded recovery keys (`:attempt`, `:state`, `:last_completed`)
  - `legacy` - decoded legacy keys (`:config_monolithic`, `:epoch_legacy`,
    `:last_recovery_legacy`)

  ## Semantics

  Updates are applied in version order; entries at or below the already-applied
  `version` are skipped, which makes application idempotent when the resolver
  re-sends a window to a new caller. Within a version, mutations apply in order
  (later mutation wins).

  - `{:set, key, value}` - parses the key and stores the (decoded) value
  - `{:clear, key}` - removes the corresponding entry
  - `{:clear_range, start_key, end_key}` - removes every known entry whose full
    system key falls within `[start_key, end_key)`
  - Unknown or unparseable system keys are ignored (forward compatibility) and
    counted in the returned stats; the same goes for `{:atomic, ...}` mutations,
    which are not supported for structured metadata.
  """

  alias Bedrock.SystemKeys

  @type family ::
          :shard_key
          | :shard
          | :materializer_key
          | :layout_log
          | :layout_services
          | :layout_id
          | :cluster
          | :cluster_policy
          | :cluster_parameter
          | :recovery
          | :legacy

  @type stats :: %{applied: non_neg_integer(), families: [family()], skipped_keys: [Bedrock.key()]}

  @type t :: %__MODULE__{
          version: Bedrock.version() | nil,
          shards: %{Bedrock.key() => term()},
          shard_metadata: %{String.t() => binary()},
          materializers: %{Bedrock.key() => binary()},
          logs: %{String.t() => term()},
          services: term() | nil,
          layout_id: term() | nil,
          cluster: %{atom() => term()},
          policies: %{atom() => term()},
          parameters: %{atom() => term()},
          recovery: %{atom() => term()},
          legacy: %{atom() => term()}
        }

  defstruct version: nil,
            shards: %{},
            shard_metadata: %{},
            materializers: %{},
            logs: %{},
            services: nil,
            layout_id: nil,
            cluster: %{},
            policies: %{},
            parameters: %{},
            recovery: %{},
            legacy: %{}

  @doc "Creates empty structured metadata."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Reduces version-ordered metadata updates into the structured metadata.

  Takes updates as returned by the resolver: a list of
  `{version, [mutation]}` entries in version order (oldest first). Entries with
  a version at or below the already-applied `version` are skipped.

  Returns `{updated_metadata, stats}` where stats counts applied mutations (with
  their key families) and skipped unknown/unsupported keys.
  """
  @spec apply_updates(t(), [{Bedrock.version(), [term()]}]) :: {t(), stats()}
  def apply_updates(%__MODULE__{} = metadata, updates) do
    initial = {metadata, %{applied: 0, families: [], skipped_keys: []}}

    {metadata, stats} =
      Enum.reduce(updates, initial, fn {version, mutations}, {metadata, stats} = acc ->
        if metadata.version != nil and version <= metadata.version do
          acc
        else
          {metadata, stats} = Enum.reduce(mutations, {metadata, stats}, &apply_mutation/2)
          {%{metadata | version: version}, stats}
        end
      end)

    {metadata,
     %{stats | families: Enum.uniq(Enum.reverse(stats.families)), skipped_keys: Enum.reverse(stats.skipped_keys)}}
  end

  # ============================================================================
  # Mutation application
  # ============================================================================

  defp apply_mutation({:set, key, value}, {metadata, stats}) do
    case SystemKeys.parse_key(key) do
      parsed when parsed in [:unknown, :error] -> skip(key, {metadata, stats})
      parsed -> applied(family_of(parsed), {put_entry(metadata, parsed, value), stats})
    end
  end

  defp apply_mutation({:clear, key}, {metadata, stats}) do
    case SystemKeys.parse_key(key) do
      parsed when parsed in [:unknown, :error] -> skip(key, {metadata, stats})
      parsed -> applied(family_of(parsed), {delete_entry(metadata, parsed), stats})
    end
  end

  defp apply_mutation({:clear_range, start_key, end_key}, {metadata, stats}) do
    in_range =
      metadata
      |> known_entries()
      |> Enum.filter(fn {full_key, _parsed} -> full_key >= start_key and full_key < end_key end)

    metadata = Enum.reduce(in_range, metadata, fn {_full_key, parsed}, metadata -> delete_entry(metadata, parsed) end)

    families = Enum.map(in_range, fn {_full_key, parsed} -> family_of(parsed) end)
    {metadata, %{stats | applied: stats.applied + 1, families: Enum.reverse(families, stats.families)}}
  end

  # Atomic operations are not supported for structured metadata
  defp apply_mutation({:atomic, _op, key, _value}, {metadata, stats}), do: skip(key, {metadata, stats})

  defp skip(key, {metadata, stats}), do: {metadata, %{stats | skipped_keys: [key | stats.skipped_keys]}}

  defp applied(family, {metadata, stats}),
    do: {metadata, %{stats | applied: stats.applied + 1, families: [family | stats.families]}}

  defp family_of({family, _param}), do: family

  defp family_of(legacy) when legacy in [:config_monolithic, :epoch_legacy, :last_recovery_legacy], do: :legacy

  defp family_of(family) when is_atom(family), do: family

  # ============================================================================
  # Entry storage (parsed key -> struct slot)
  # ============================================================================

  # Values written by PersistencePhase are term_to_binary encoded, except
  # shard/1 (FlatBuffer ShardMetadata) and materializer_key/1, which are kept
  # encoded. Encoding will change under bedrock-ri40; decode stays centralized here.
  defp put_entry(metadata, {:shard_key, end_key}, value),
    do: %{metadata | shards: Map.put(metadata.shards, end_key, decode(value))}

  defp put_entry(metadata, {:shard, tag}, value),
    do: %{metadata | shard_metadata: Map.put(metadata.shard_metadata, tag, value)}

  defp put_entry(metadata, {:materializer_key, end_key}, value),
    do: %{metadata | materializers: Map.put(metadata.materializers, end_key, value)}

  defp put_entry(metadata, {:layout_log, log_id}, value),
    do: %{metadata | logs: Map.put(metadata.logs, log_id, decode(value))}

  defp put_entry(metadata, :layout_services, value), do: %{metadata | services: decode(value)}
  defp put_entry(metadata, :layout_id, value), do: %{metadata | layout_id: decode(value)}

  defp put_entry(metadata, {:cluster, name}, value),
    do: %{metadata | cluster: Map.put(metadata.cluster, name, decode(value))}

  defp put_entry(metadata, {:cluster_policy, name}, value),
    do: %{metadata | policies: Map.put(metadata.policies, name, decode(value))}

  defp put_entry(metadata, {:cluster_parameter, name}, value),
    do: %{metadata | parameters: Map.put(metadata.parameters, name, decode(value))}

  defp put_entry(metadata, {:recovery, name}, value),
    do: %{metadata | recovery: Map.put(metadata.recovery, name, decode(value))}

  defp put_entry(metadata, legacy, value) when legacy in [:config_monolithic, :epoch_legacy, :last_recovery_legacy],
    do: %{metadata | legacy: Map.put(metadata.legacy, legacy, decode(value))}

  defp delete_entry(metadata, {:shard_key, end_key}), do: %{metadata | shards: Map.delete(metadata.shards, end_key)}

  defp delete_entry(metadata, {:shard, tag}), do: %{metadata | shard_metadata: Map.delete(metadata.shard_metadata, tag)}

  defp delete_entry(metadata, {:materializer_key, end_key}),
    do: %{metadata | materializers: Map.delete(metadata.materializers, end_key)}

  defp delete_entry(metadata, {:layout_log, log_id}), do: %{metadata | logs: Map.delete(metadata.logs, log_id)}
  defp delete_entry(metadata, :layout_services), do: %{metadata | services: nil}
  defp delete_entry(metadata, :layout_id), do: %{metadata | layout_id: nil}
  defp delete_entry(metadata, {:cluster, name}), do: %{metadata | cluster: Map.delete(metadata.cluster, name)}
  defp delete_entry(metadata, {:cluster_policy, name}), do: %{metadata | policies: Map.delete(metadata.policies, name)}

  defp delete_entry(metadata, {:cluster_parameter, name}),
    do: %{metadata | parameters: Map.delete(metadata.parameters, name)}

  defp delete_entry(metadata, {:recovery, name}), do: %{metadata | recovery: Map.delete(metadata.recovery, name)}

  defp delete_entry(metadata, legacy) when legacy in [:config_monolithic, :epoch_legacy, :last_recovery_legacy],
    do: %{metadata | legacy: Map.delete(metadata.legacy, legacy)}

  defp decode(value), do: :erlang.binary_to_term(value)

  # ============================================================================
  # clear_range support: enumerate every stored entry with its full system key
  # ============================================================================

  defp known_entries(metadata) do
    Enum.concat([
      Enum.map(metadata.shards, fn {end_key, _} -> {SystemKeys.shard_key(end_key), {:shard_key, end_key}} end),
      Enum.map(metadata.shard_metadata, fn {tag, _} -> {SystemKeys.shards_prefix() <> tag, {:shard, tag}} end),
      Enum.map(metadata.materializers, fn {end_key, _} ->
        {SystemKeys.materializer_key(end_key), {:materializer_key, end_key}}
      end),
      Enum.map(metadata.logs, fn {log_id, _} -> {SystemKeys.layout_log(log_id), {:layout_log, log_id}} end),
      if(metadata.services == nil, do: [], else: [{SystemKeys.layout_services(), :layout_services}]),
      if(metadata.layout_id == nil, do: [], else: [{SystemKeys.layout_id(), :layout_id}]),
      Enum.map(metadata.cluster, fn {name, _} -> {cluster_key(name), {:cluster, name}} end),
      Enum.map(metadata.policies, fn {name, _} -> {policy_key(name), {:cluster_policy, name}} end),
      Enum.map(metadata.parameters, fn {name, _} -> {parameter_key(name), {:cluster_parameter, name}} end),
      Enum.map(metadata.recovery, fn {name, _} -> {recovery_key(name), {:recovery, name}} end),
      Enum.map(metadata.legacy, fn {name, _} -> {legacy_key(name), name} end)
    ])
  end

  defp cluster_key(:coordinators), do: SystemKeys.cluster_coordinators()
  defp cluster_key(:epoch), do: SystemKeys.cluster_epoch()

  defp policy_key(:volunteer_nodes), do: SystemKeys.cluster_policies_volunteer_nodes()

  defp parameter_key(:desired_logs), do: SystemKeys.cluster_parameters_desired_logs()
  defp parameter_key(:desired_replication), do: SystemKeys.cluster_parameters_desired_replication()
  defp parameter_key(:desired_commit_proxies), do: SystemKeys.cluster_parameters_desired_commit_proxies()
  defp parameter_key(:desired_coordinators), do: SystemKeys.cluster_parameters_desired_coordinators()
  defp parameter_key(:desired_read_version_proxies), do: SystemKeys.cluster_parameters_desired_read_version_proxies()

  defp parameter_key(:empty_transaction_timeout_ms), do: SystemKeys.cluster_parameters_empty_transaction_timeout_ms()

  defp parameter_key(:ping_rate_in_hz), do: SystemKeys.cluster_parameters_ping_rate_in_hz()
  defp parameter_key(:retransmission_rate_in_hz), do: SystemKeys.cluster_parameters_retransmission_rate_in_hz()
  defp parameter_key(:transaction_window_in_ms), do: SystemKeys.cluster_parameters_transaction_window_in_ms()

  defp recovery_key(:attempt), do: SystemKeys.recovery_attempt()
  defp recovery_key(:state), do: SystemKeys.recovery_state()
  defp recovery_key(:last_completed), do: SystemKeys.recovery_last_completed()

  defp legacy_key(:config_monolithic), do: SystemKeys.config_monolithic()
  defp legacy_key(:epoch_legacy), do: SystemKeys.epoch_legacy()
  defp legacy_key(:last_recovery_legacy), do: SystemKeys.last_recovery_legacy()
end
