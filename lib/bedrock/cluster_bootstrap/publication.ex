defmodule Bedrock.ClusterBootstrap.Publication do
  @moduledoc "Validated bootstrap reads and generation-bound publication authority."
  alias Bedrock.ClusterBootstrap.Discovery
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.SystemKeys.ClusterBootstrap

  @max_generation 0xFFFFFFFFFFFFFFFF

  @spec location(module()) :: {:ok, ObjectStorage.backend(), String.t()} | {:error, term()}
  def location(cluster) do
    config = cluster.node_config()
    backend = Keyword.get(config, :object_storage) || derived_backend(config)
    key = Keyword.get(config, :bootstrap_key) || ObjectStorage.Config.bootstrap_key() || "bootstrap"
    if backend, do: {:ok, backend, key}, else: {:error, :no_object_storage}
  end

  defp derived_backend(config) do
    path =
      Enum.find_value([:coordinator, :log, :storage, :materializer, :coordination], fn role ->
        config |> Keyword.get(role, []) |> Keyword.get(:path)
      end)

    if path, do: {LocalFilesystem, root: Path.join(path, "object_storage")}
  end

  @spec load(module()) :: {:ok, map()} | {:error, term()}
  def load(cluster) do
    with {:ok, backend, key} <- location(cluster) do
      case read(backend, key) do
        {:error, :not_found} ->
          initial = node() |> Discovery.create_initial() |> ClusterBootstrap.to_binary()

          case ObjectStorage.put_if_not_exists(backend, key, initial) do
            :ok -> read(backend, key)
            {:error, :already_exists} -> read(backend, key)
            error -> error
          end

        result ->
          result
      end
    end
  end

  @spec read(ObjectStorage.backend(), String.t()) :: {:ok, map()} | {:error, term()}
  def read(backend, key) do
    with {:ok, bytes, token} <- ObjectStorage.get_with_version(backend, key),
         :ok <- require_true(text?(token), :missing_version_token),
         {:ok, bootstrap} <- decode(bytes) do
      {:ok, %{backend: backend, key: key, bytes: bytes, version_token: token, bootstrap: bootstrap}}
    end
  end

  @spec decode(binary()) :: {:ok, map()} | {:error, term()}
  def decode(bytes) do
    with {:ok, bootstrap} <- ClusterBootstrap.read(bytes), :ok <- validate(bootstrap), do: {:ok, bootstrap}
  rescue
    _ -> {:error, :invalid_bootstrap}
  catch
    _, _ -> {:error, :invalid_bootstrap}
  end

  @spec validate(map()) :: :ok | {:error, term()}
  def validate(bootstrap) do
    version = Map.get(bootstrap, :protocol_version, 0)

    cond do
      version not in [0, 1] ->
        {:error, {:unsupported_bootstrap_protocol, version}}

      not base_valid?(bootstrap) ->
        {:error, :invalid_bootstrap}

      version == 0 ->
        validate_legacy(bootstrap)

      true ->
        validate_current(bootstrap)
    end
  end

  defp validate_legacy(b) do
    identity_absent = Enum.all?([:recovery_id, :publication_id], &(Map.get(b, &1) in [nil, ""]))
    completed_valid = if b.logs == [], do: b.epoch in [0, 1], else: b.epoch > 0

    if identity_absent and Map.get(b, :recovery_generation, 0) == 0 and completed_valid,
      do: :ok,
      else: {:error, :invalid_bootstrap}
  end

  defp base_valid?(b) do
    text?(b[:cluster_id]) and uint?(b[:epoch]) and
      records?(b[:logs], [:id]) and
      records?(b[:coordinators], [:node]) and b.coordinators != [] and
      records?(b[:system_materializers] || [], [:id, :node]) and
      parameters_valid?(b[:parameters]) and policies_valid?(b[:policies])
  end

  defp records?(records, keys) when is_list(records) do
    Enum.all?(records, fn record ->
      is_map(record) and Enum.all?(keys, &text?(record[&1]))
    end) and length(Enum.uniq_by(records, &Map.get(&1, hd(keys)))) == length(records)
  end

  defp records?(_, _), do: false

  defp parameters_valid?(nil), do: true

  defp parameters_valid?(params) when is_map(params) do
    Enum.all?(
      [
        :desired_logs,
        :desired_replication_factor,
        :desired_commit_proxies,
        :desired_coordinators,
        :desired_read_version_proxies,
        :ping_rate_in_hz,
        :retransmission_rate_in_hz,
        :transaction_window_in_ms
      ],
      fn key ->
        not Map.has_key?(params, key) or
          (is_integer(params[key]) and params[key] > 0 and params[key] <= 0xFFFFFFFF)
      end
    ) and
      (not Map.has_key?(params, :empty_transaction_timeout_ms) or
         (uint?(params.empty_transaction_timeout_ms) and params.empty_transaction_timeout_ms <= 0xFFFFFFFF))
  end

  defp parameters_valid?(_), do: false

  defp policies_valid?(nil), do: true

  defp policies_valid?(policies) when is_map(policies),
    do: not Map.has_key?(policies, :allow_volunteer_nodes_to_join) or is_boolean(policies.allow_volunteer_nodes_to_join)

  defp policies_valid?(_), do: false

  defp validate_current(b) do
    if uint?(b[:recovery_generation]) and text?(b[:recovery_id]) do
      validate_identity(b, identity_kind(b))
    else
      {:error, :invalid_bootstrap}
    end
  end

  defp identity_kind(%{logs: []}), do: :fresh
  defp identity_kind(%{epoch: 0}), do: :invalid
  defp identity_kind(%{recovery_generation: generation, epoch: epoch}) when generation > epoch, do: :reserved
  defp identity_kind(%{recovery_generation: same, epoch: same}), do: :completed
  defp identity_kind(_), do: :invalid

  defp validate_identity(b, :fresh) do
    if b.epoch in [0, 1] and b.recovery_generation > b.epoch and b[:publication_id] in [nil, ""],
      do: :ok,
      else: {:error, :invalid_bootstrap}
  end

  defp validate_identity(b, :reserved) do
    if b[:publication_id] in [nil, ""] or (text?(b[:publication_id]) and b.publication_id != b.recovery_id),
      do: :ok,
      else: {:error, :invalid_bootstrap}
  end

  defp validate_identity(b, :completed) do
    if text?(b[:publication_id]) and b.publication_id == b.recovery_id, do: :ok, else: {:error, :invalid_bootstrap}
  end

  defp validate_identity(_b, :invalid), do: {:error, :invalid_bootstrap}

  defp require_true(true, _reason), do: :ok
  defp require_true(false, reason), do: {:error, reason}

  defp text?(value), do: is_binary(value) and byte_size(value) > 0
  defp uint?(value), do: is_integer(value) and value >= 0 and value <= @max_generation
  @spec generation_floor(map()) :: non_neg_integer()
  def generation_floor(b), do: max(b.epoch, Map.get(b, :recovery_generation, 0))

  @spec reserve(ObjectStorage.backend(), String.t(), pos_integer(), binary(), binary()) ::
          {:ok, map()} | {:error, term()}
  def reserve(backend, key, generation, request_id, cluster_id),
    do: reserve(backend, key, generation, request_id, cluster_id, 3)

  defp reserve(_backend, _key, _generation, _request_id, _cluster_id, 0), do: {:error, :reservation_conflicts}

  defp reserve(backend, key, generation, request_id, cluster_id, attempts) do
    with {:ok, current} <- read(backend, key),
         :ok <- require_true(current.bootstrap.cluster_id == cluster_id, :cluster_identity_changed),
         :ok <- require_true(generation > generation_floor(current.bootstrap), :superseded) do
      reserved =
        Map.merge(current.bootstrap, %{protocol_version: 1, recovery_generation: generation, recovery_id: request_id})

      with :ok <- validate(reserved) do
        bytes = ClusterBootstrap.to_binary(reserved)
        result = ObjectStorage.put_if_version_matches(backend, key, current.version_token, bytes)

        case exact_read(backend, key, bytes) do
          {:ok, verified} ->
            {:ok,
             %{
               backend: backend,
               key: key,
               generation: generation,
               recovery_id: request_id,
               version_token: verified.version_token,
               reserved_bytes: bytes,
               prior_bootstrap: current.bootstrap
             }}

          _ when result == {:error, :version_mismatch} ->
            reserve(backend, key, generation, request_id, cluster_id, attempts - 1)

          {:error, reason} ->
            {:error, {:reservation_unverified, reason}}
        end
      end
    end
  end

  @spec exact_read(ObjectStorage.backend(), String.t(), binary()) :: {:ok, map()} | {:error, term()}
  def exact_read(backend, key, expected) do
    with {:ok, current} <- read(backend, key),
         :ok <- require_true(current.bytes == expected, :publication_mismatch),
         do: {:ok, current}
  end

  @spec publish(map(), map()) :: :ok | {:error, term()}
  def publish(reservation, bootstrap) do
    with :ok <-
           require_true(
             bootstrap.epoch == reservation.generation and
               bootstrap.recovery_generation == reservation.generation and
               bootstrap.recovery_id == reservation.recovery_id and
               bootstrap.publication_id == reservation.recovery_id,
             :invalid_publication_identity
           ),
         :ok <- validate(bootstrap) do
      bytes = ClusterBootstrap.to_binary(bootstrap)

      case ObjectStorage.put_if_version_matches(reservation.backend, reservation.key, reservation.version_token, bytes) do
        :ok ->
          :ok

        {:error, _} ->
          case exact_read(reservation.backend, reservation.key, bytes) do
            {:ok, _} -> :ok
            {:error, reason} -> {:error, reason}
          end
      end
    end
  end

  # Build a Config struct from ClusterBootstrap data
  @spec config(map(), module()) :: map()
  def config(bootstrap, cluster) do
    {:ok, coordinator_nodes} = cluster.fetch_coordinator_nodes()

    %{
      coordinators: coordinator_nodes,
      parameters: build_parameters(bootstrap[:parameters], coordinator_nodes),
      policies: build_policies(bootstrap[:policies])
    }
  end

  defp build_parameters(nil, coordinator_nodes), do: default_parameters(coordinator_nodes)

  defp build_parameters(params, coordinator_nodes) do
    defaults = default_parameters(coordinator_nodes)

    defaults
    |> Map.put(:empty_transaction_timeout_ms, 0)
    |> Map.new(fn {key, default} -> {key, Map.get(params, key) || default} end)
    |> Map.put(:nodes, coordinator_nodes)
  end

  defp default_parameters(coordinator_nodes) do
    %{
      nodes: coordinator_nodes,
      desired_coordinators: length(coordinator_nodes),
      desired_logs: 1,
      desired_replication_factor: 1,
      desired_commit_proxies: 1,
      desired_read_version_proxies: 1,
      ping_rate_in_hz: 10,
      retransmission_rate_in_hz: 20,
      transaction_window_in_ms: 5_000
    }
  end

  defp build_policies(nil), do: %{allow_volunteer_nodes_to_join: true}
  defp build_policies(p), do: %{allow_volunteer_nodes_to_join: p[:allow_volunteer_nodes_to_join] || false}
end
