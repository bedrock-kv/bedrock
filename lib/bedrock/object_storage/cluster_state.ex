defmodule Bedrock.ObjectStorage.ClusterState do
  @moduledoc """
  Cluster state persistence using object storage.

  This module provides functions to save and load cluster configuration from
  object storage. It enables stateless coordinators by allowing cluster state
  to be loaded from object storage on cold start.

  ## Storage Format

  Cluster state is stored at path `{cluster_name}/state` using Erlang binary
  serialization (term_to_binary). The stored data is a tuple of:

      {epoch, encoded_config}

  Where `encoded_config` has all PIDs converted to `{otp_name, node}` tuples
  for safe serialization across restarts.

  ## Usage

      # After successful recovery
      :ok = ClusterState.save(backend, cluster_name, epoch, config, cluster_module)

      # On coordinator cold start
      case ClusterState.load(backend, cluster_name, cluster_module) do
        {:ok, epoch, config} -> start_with_state(epoch, config)
        {:error, :not_found} -> initialize_new_cluster()
      end
  """

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.Persistence
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Keys

  @type epoch :: non_neg_integer()
  @type cluster_name :: String.t()
  @type cluster_module :: module()

  @doc """
  Saves cluster configuration to object storage.

  This should be called after successful recovery to persist the cluster state.
  The configuration is encoded using `Persistence.encode_for_storage/2` to convert
  PIDs to serializable OTP references.

  ## Parameters

  - `backend` - ObjectStorage backend reference
  - `cluster_name` - Name of the cluster (used in storage path)
  - `epoch` - Current recovery epoch
  - `config` - Cluster configuration to save
  - `cluster_module` - Module implementing cluster callbacks (for OTP name resolution)

  ## Returns

  - `:ok` - State saved successfully
  - `{:error, reason}` - Save failed
  """
  @spec save(
          backend :: ObjectStorage.backend(),
          cluster_name :: cluster_name(),
          epoch :: epoch(),
          config :: Config.t(),
          cluster_module :: cluster_module()
        ) :: :ok | {:error, term()}
  def save(backend, cluster_name, epoch, config, cluster_module) do
    encoded_config = Persistence.encode_for_storage(config, cluster_module)
    data = :erlang.term_to_binary({epoch, encoded_config})
    key = Keys.cluster_state_path(cluster_name)

    ObjectStorage.put(backend, key, data)
  end

  @doc """
  Loads cluster configuration from object storage.

  This should be called during coordinator cold start to restore previous
  cluster state. The configuration is decoded using `Persistence.decode_from_storage/2`
  to convert OTP references back to PIDs.

  Note: PIDs are resolved at load time, so processes must be running for
  resolution to succeed. Non-running processes will have `nil` PIDs.

  ## Parameters

  - `backend` - ObjectStorage backend reference
  - `cluster_name` - Name of the cluster (used in storage path)
  - `cluster_module` - Module implementing cluster callbacks (for OTP name resolution)

  ## Returns

  - `{:ok, epoch, config}` - State loaded and decoded successfully
  - `{:error, :not_found}` - No saved state exists
  - `{:error, reason}` - Load failed
  """
  @spec load(
          backend :: ObjectStorage.backend(),
          cluster_name :: cluster_name(),
          cluster_module :: cluster_module()
        ) :: {:ok, epoch(), Config.t()} | {:error, :not_found | term()}
  def load(backend, cluster_name, cluster_module) do
    key = Keys.cluster_state_path(cluster_name)

    case ObjectStorage.get(backend, key) do
      {:ok, data} ->
        {epoch, encoded_config} = :erlang.binary_to_term(data)
        config = Persistence.decode_from_storage(encoded_config, cluster_module)
        {:ok, epoch, config}

      {:error, :not_found} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Loads raw cluster state without PID resolution.

  This is useful when you need to inspect the stored state without resolving
  PIDs, or when processes are not running yet.

  ## Returns

  - `{:ok, epoch, encoded_config}` - Raw state loaded (OTP references not resolved)
  - `{:error, :not_found}` - No saved state exists
  - `{:error, reason}` - Load failed
  """
  @spec load_raw(
          backend :: ObjectStorage.backend(),
          cluster_name :: cluster_name()
        ) :: {:ok, epoch(), Persistence.encoded_config()} | {:error, :not_found | term()}
  def load_raw(backend, cluster_name) do
    key = Keys.cluster_state_path(cluster_name)

    case ObjectStorage.get(backend, key) do
      {:ok, data} ->
        {epoch, encoded_config} = :erlang.binary_to_term(data)
        {:ok, epoch, encoded_config}

      {:error, :not_found} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Checks if cluster state exists in object storage.

  ## Returns

  - `true` - State exists
  - `false` - No state exists or error checking
  """
  @spec exists?(backend :: ObjectStorage.backend(), cluster_name :: cluster_name()) :: boolean()
  def exists?(backend, cluster_name) do
    key = Keys.cluster_state_path(cluster_name)

    case ObjectStorage.get(backend, key) do
      {:ok, _data} -> true
      {:error, _} -> false
    end
  end

  @doc """
  Deletes cluster state from object storage.

  ## Returns

  - `:ok` - State deleted (or didn't exist)
  - `{:error, reason}` - Delete failed
  """
  @spec delete(backend :: ObjectStorage.backend(), cluster_name :: cluster_name()) ::
          :ok | {:error, term()}
  def delete(backend, cluster_name) do
    key = Keys.cluster_state_path(cluster_name)
    ObjectStorage.delete(backend, key)
  end
end
