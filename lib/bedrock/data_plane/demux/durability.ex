defmodule Bedrock.DataPlane.Demux.Durability do
  @moduledoc """
  Tracks durable versions across shards using gb_sets for O(log N) min extraction.

  The Demux needs to track the minimum durable version across all active ShardServers
  to know when it's safe for the Log to trim WAL segments. This module provides
  efficient tracking with:

  - O(log N) min extraction via `:gb_sets`
  - O(log N) version updates
  - Shard activation tracking

  ## Implementation

  We maintain both a map of `shard_id => durable_version` for lookups and a
  `:gb_sets` ordered set of `{version, shard_id}` tuples for efficient min extraction.

  When a shard's durable version advances, we:
  1. Remove the old `{old_version, shard_id}` from gb_sets
  2. Insert the new `{new_version, shard_id}` into gb_sets
  3. Update the map

  ## Usage

      durability = Durability.new()

      # Activate a shard with initial version
      durability = Durability.activate_shard(durability, 0, 1000)

      # Update when shard reports durability
      durability = Durability.update_shard(durability, 0, 2000)

      # Get minimum durable version
      Durability.min_durable_version(durability)
      #=> 2000
  """

  @type shard_id :: non_neg_integer()
  @type version :: Bedrock.version()

  @type t :: %__MODULE__{
          shard_versions: %{shard_id() => version()},
          version_index: :gb_sets.set({version(), shard_id()})
        }

  defstruct shard_versions: %{},
            version_index: :gb_sets.new()

  @doc """
  Creates a new empty durability tracker.
  """
  @spec new() :: t()
  def new do
    %__MODULE__{}
  end

  @doc """
  Activates a shard with an initial durable version.

  Called when the first transaction touches a shard. The shard starts
  with `initial_version` as its durable version (typically the last_seen_version
  at the time of activation).

  Returns `{:ok, updated_durability}` or `{:error, :already_active}` if shard
  is already being tracked.
  """
  @spec activate_shard(t(), shard_id(), version()) :: {:ok, t()} | {:error, :already_active}
  def activate_shard(%__MODULE__{} = durability, shard_id, initial_version) do
    if Map.has_key?(durability.shard_versions, shard_id) do
      {:error, :already_active}
    else
      updated = %{
        durability
        | shard_versions: Map.put(durability.shard_versions, shard_id, initial_version),
          version_index: :gb_sets.add({initial_version, shard_id}, durability.version_index)
      }

      {:ok, updated}
    end
  end

  @doc """
  Updates a shard's durable version.

  Called when a ShardServer reports that it has durably written data up to
  a certain version. The version must be >= the current durable version for
  that shard.

  Returns `{:ok, updated_durability}` or an error if the shard isn't tracked
  or the version is going backwards.
  """
  @spec update_shard(t(), shard_id(), version()) :: {:ok, t()} | {:error, :not_active | :version_going_backwards}
  def update_shard(%__MODULE__{} = durability, shard_id, new_version) do
    case Map.fetch(durability.shard_versions, shard_id) do
      :error ->
        {:error, :not_active}

      {:ok, old_version} when new_version < old_version ->
        {:error, :version_going_backwards}

      {:ok, old_version} when new_version == old_version ->
        # No change needed
        {:ok, durability}

      {:ok, old_version} ->
        # Remove old entry, add new entry
        new_index =
          durability.version_index
          |> then(&:gb_sets.delete({old_version, shard_id}, &1))
          |> then(&:gb_sets.add({new_version, shard_id}, &1))

        updated = %{
          durability
          | shard_versions: Map.put(durability.shard_versions, shard_id, new_version),
            version_index: new_index
        }

        {:ok, updated}
    end
  end

  @doc """
  Returns the minimum durable version across all tracked shards.

  Returns `nil` if no shards are being tracked.
  """
  @spec min_durable_version(t()) :: version() | nil
  def min_durable_version(%__MODULE__{version_index: index}) do
    if :gb_sets.is_empty(index) do
      nil
    else
      {version, _shard_id} = :gb_sets.smallest(index)
      version
    end
  end

  @doc """
  Returns the durable version for a specific shard.

  Returns `nil` if the shard is not being tracked.
  """
  @spec shard_version(t(), shard_id()) :: version() | nil
  def shard_version(%__MODULE__{shard_versions: versions}, shard_id) do
    Map.get(versions, shard_id)
  end

  @doc """
  Returns true if the given shard is being tracked.
  """
  @spec active?(t(), shard_id()) :: boolean()
  def active?(%__MODULE__{shard_versions: versions}, shard_id) do
    Map.has_key?(versions, shard_id)
  end

  @doc """
  Returns the number of active shards being tracked.
  """
  @spec active_shard_count(t()) :: non_neg_integer()
  def active_shard_count(%__MODULE__{shard_versions: versions}) do
    map_size(versions)
  end

  @doc """
  Returns all active shard IDs.
  """
  @spec active_shards(t()) :: [shard_id()]
  def active_shards(%__MODULE__{shard_versions: versions}) do
    Map.keys(versions)
  end

  @doc """
  Deactivates a shard, removing it from durability tracking.

  This is typically used during cleanup or when a shard is no longer needed.
  Returns the updated durability tracker.
  """
  @spec deactivate_shard(t(), shard_id()) :: t()
  def deactivate_shard(%__MODULE__{} = durability, shard_id) do
    case Map.fetch(durability.shard_versions, shard_id) do
      :error ->
        # Not tracked, nothing to do
        durability

      {:ok, version} ->
        %{
          durability
          | shard_versions: Map.delete(durability.shard_versions, shard_id),
            version_index: :gb_sets.delete_any({version, shard_id}, durability.version_index)
        }
    end
  end
end
