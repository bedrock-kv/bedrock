defmodule Bedrock.ObjectStorage.Keys do
  @moduledoc """
  Key formatting helpers for object storage.

  Object stores list keys in ascending lexicographic order. To efficiently
  retrieve the newest objects first, we use inverted version numbers in keys:

      inverted = (2^64 - 1) - version

  This ensures that newer versions (higher numbers) sort first.

  ## Path Structure

  - Cluster state: `/{cluster}/state`
  - Chunks: `/{cluster}/shards/{tag}/chunks/{inverted_version}`
  - Snapshots: `/{cluster}/shards/{tag}/snapshots/{inverted_version}`
  """

  @max_version 0xFFFFFFFFFFFFFFFF

  @doc """
  Converts a version to an inverted version for sorting.

  Inverted versions sort in descending order (newest first).

  ## Examples

      iex> Keys.invert_version(0)
      18446744073709551615

      iex> Keys.invert_version(18446744073709551615)
      0

      iex> Keys.invert_version(1000)
      18446744073709550615
  """
  @spec invert_version(non_neg_integer()) :: non_neg_integer()
  def invert_version(version) when is_integer(version) and version >= 0 and version <= @max_version do
    @max_version - version
  end

  @doc """
  Converts an inverted version back to the original version.

  ## Examples

      iex> Keys.restore_version(18446744073709551615)
      0

      iex> Keys.restore_version(0)
      18446744073709551615
  """
  @spec restore_version(non_neg_integer()) :: non_neg_integer()
  def restore_version(inverted) when is_integer(inverted) and inverted >= 0 and inverted <= @max_version do
    @max_version - inverted
  end

  @doc """
  Formats an inverted version as a zero-padded string for lexicographic sorting.

  The string is 20 characters wide (max uint64 is 20 digits).

  ## Examples

      iex> Keys.format_inverted_version(0)
      "00000000000000000000"

      iex> Keys.format_inverted_version(1000)
      "00000000000000001000"
  """
  @spec format_inverted_version(non_neg_integer()) :: String.t()
  def format_inverted_version(inverted) when is_integer(inverted) and inverted >= 0 do
    inverted
    |> Integer.to_string()
    |> String.pad_leading(20, "0")
  end

  @doc """
  Parses a zero-padded inverted version string back to an integer.

  ## Examples

      iex> Keys.parse_inverted_version("00000000000000000000")
      {:ok, 0}

      iex> Keys.parse_inverted_version("00000000000000001000")
      {:ok, 1000}

      iex> Keys.parse_inverted_version("invalid")
      {:error, :invalid_format}
  """
  @spec parse_inverted_version(String.t()) :: {:ok, non_neg_integer()} | {:error, :invalid_format}
  def parse_inverted_version(str) when is_binary(str) do
    case Integer.parse(str) do
      {value, ""} when value >= 0 -> {:ok, value}
      _ -> {:error, :invalid_format}
    end
  end

  @doc """
  Formats a version as an inverted, zero-padded string key component.

  Combines `invert_version/1` and `format_inverted_version/1`.

  ## Examples

      iex> Keys.version_to_key(0)
      "18446744073709551615"

      iex> Keys.version_to_key(18446744073709551615)
      "00000000000000000000"
  """
  @spec version_to_key(non_neg_integer()) :: String.t()
  def version_to_key(version) do
    version
    |> invert_version()
    |> format_inverted_version()
  end

  @doc """
  Parses a key component back to the original version.

  Combines `parse_inverted_version/1` and `restore_version/1`.

  ## Examples

      iex> Keys.key_to_version("18446744073709551615")
      {:ok, 0}

      iex> Keys.key_to_version("00000000000000000000")
      {:ok, 18446744073709551615}
  """
  @spec key_to_version(String.t()) :: {:ok, non_neg_integer()} | {:error, :invalid_format}
  def key_to_version(key) do
    case parse_inverted_version(key) do
      {:ok, inverted} -> {:ok, restore_version(inverted)}
      error -> error
    end
  end

  # Path builders

  @doc """
  Builds a cluster state path.

  ## Examples

      iex> Keys.cluster_state_path("my-cluster")
      "my-cluster/state"
  """
  @spec cluster_state_path(String.t()) :: String.t()
  def cluster_state_path(cluster) when is_binary(cluster) do
    "#{cluster}/state"
  end

  @doc """
  Builds a chunk path for a shard.

  ## Examples

      iex> Keys.chunk_path("my-cluster", "shard-01", 1000)
      "my-cluster/shards/shard-01/chunks/18446744073709550615"
  """
  @spec chunk_path(String.t(), String.t(), non_neg_integer()) :: String.t()
  def chunk_path(cluster, shard_tag, highest_version) when is_binary(cluster) and is_binary(shard_tag) do
    "#{cluster}/shards/#{shard_tag}/chunks/#{version_to_key(highest_version)}"
  end

  @doc """
  Builds a chunks prefix for listing all chunks in a shard.

  ## Examples

      iex> Keys.chunks_prefix("my-cluster", "shard-01")
      "my-cluster/shards/shard-01/chunks/"
  """
  @spec chunks_prefix(String.t(), String.t()) :: String.t()
  def chunks_prefix(cluster, shard_tag) when is_binary(cluster) and is_binary(shard_tag) do
    "#{cluster}/shards/#{shard_tag}/chunks/"
  end

  @doc """
  Builds a snapshot path for a shard.

  ## Examples

      iex> Keys.snapshot_path("my-cluster", "shard-01", 1000)
      "my-cluster/shards/shard-01/snapshots/18446744073709550615"
  """
  @spec snapshot_path(String.t(), String.t(), non_neg_integer()) :: String.t()
  def snapshot_path(cluster, shard_tag, version) when is_binary(cluster) and is_binary(shard_tag) do
    "#{cluster}/shards/#{shard_tag}/snapshots/#{version_to_key(version)}"
  end

  @doc """
  Builds a snapshots prefix for listing all snapshots in a shard.

  ## Examples

      iex> Keys.snapshots_prefix("my-cluster", "shard-01")
      "my-cluster/shards/shard-01/snapshots/"
  """
  @spec snapshots_prefix(String.t(), String.t()) :: String.t()
  def snapshots_prefix(cluster, shard_tag) when is_binary(cluster) and is_binary(shard_tag) do
    "#{cluster}/shards/#{shard_tag}/snapshots/"
  end

  @doc """
  Extracts the version from a chunk or snapshot key.

  ## Examples

      iex> Keys.extract_version("my-cluster/shards/shard-01/chunks/18446744073709550615")
      {:ok, 1000}

      iex> Keys.extract_version("invalid")
      {:error, :invalid_format}
  """
  @spec extract_version(String.t()) :: {:ok, non_neg_integer()} | {:error, :invalid_format}
  def extract_version(key) when is_binary(key) do
    key
    |> Path.basename()
    |> key_to_version()
  end
end
