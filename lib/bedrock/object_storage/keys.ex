defmodule Bedrock.ObjectStorage.Keys do
  @moduledoc """
  Key formatting helpers for object storage.

  Object stores list keys in ascending lexicographic order. To efficiently
  retrieve the newest objects first, we use inverted version numbers in keys:

      inverted = (2^64 - 1) - version

  This ensures that newer versions (higher numbers) sort first.

  Version numbers are encoded in base36 (0-9, a-z) for compact filenames.
  A 64-bit integer requires 13 base36 characters.

  ## Path Structure

  Paths are relative to the object storage root, which is already cluster-scoped:

  - Cluster state: `state`
  - Chunks: `c/{shard}/{version}`
  - Snapshots: `s/{shard}/{version}`
  """

  @max_version 0xFFFFFFFFFFFFFFFF
  # max uint64 in base36 is 13 chars
  @base36_width 13

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
  Formats an inverted version as a base36-encoded string for lexicographic sorting.

  The string is 13 characters wide, zero-padded.

  ## Examples

      iex> Keys.format_inverted_version(0)
      "0000000000000"

      iex> Keys.format_inverted_version(1000)
      "00000000000rs"
  """
  @spec format_inverted_version(non_neg_integer()) :: String.t()
  def format_inverted_version(inverted) when is_integer(inverted) and inverted >= 0 do
    inverted
    |> Integer.to_string(36)
    |> String.downcase()
    |> String.pad_leading(@base36_width, "0")
  end

  @doc """
  Parses a base36-encoded inverted version string back to an integer.

  ## Examples

      iex> Keys.parse_inverted_version("0000000000000")
      {:ok, 0}

      iex> Keys.parse_inverted_version("00000000000rs")
      {:ok, 1000}

      iex> Keys.parse_inverted_version("invalid!")
      {:error, :invalid_format}
  """
  @spec parse_inverted_version(String.t()) :: {:ok, non_neg_integer()} | {:error, :invalid_format}
  def parse_inverted_version(str) when is_binary(str) do
    case Integer.parse(str, 36) do
      {value, ""} when value >= 0 -> {:ok, value}
      _ -> {:error, :invalid_format}
    end
  end

  @doc """
  Formats a version as an inverted, base36-encoded string key component.

  Combines `invert_version/1` and `format_inverted_version/1`.

  ## Examples

      iex> Keys.version_to_key(0)
      "3w5e11264sgsf"

      iex> Keys.version_to_key(18446744073709551615)
      "0000000000000"
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

      iex> Keys.key_to_version("3w5e11264sgsf")
      {:ok, 0}

      iex> Keys.key_to_version("0000000000000")
      {:ok, 18446744073709551615}
  """
  @spec key_to_version(String.t()) :: {:ok, non_neg_integer()} | {:error, :invalid_format}
  def key_to_version(key) do
    case parse_inverted_version(key) do
      {:ok, inverted} -> {:ok, restore_version(inverted)}
      error -> error
    end
  end

  # Shard tag formatting

  @doc """
  Formats a shard ID as a base36 string for use in paths.

  ## Examples

      iex> Keys.shard_tag(0)
      "0"

      iex> Keys.shard_tag(35)
      "z"

      iex> Keys.shard_tag(1000)
      "rs"
  """
  @spec shard_tag(non_neg_integer()) :: String.t()
  def shard_tag(shard_id) when is_integer(shard_id) and shard_id >= 0 do
    shard_id |> Integer.to_string(36) |> String.downcase()
  end

  @doc """
  Parses a shard tag back to an integer shard ID.

  ## Examples

      iex> Keys.parse_shard_tag("0")
      {:ok, 0}

      iex> Keys.parse_shard_tag("z")
      {:ok, 35}

      iex> Keys.parse_shard_tag("rs")
      {:ok, 1000}
  """
  @spec parse_shard_tag(String.t()) :: {:ok, non_neg_integer()} | {:error, :invalid_format}
  def parse_shard_tag(tag) when is_binary(tag) do
    case Integer.parse(tag, 36) do
      {value, ""} when value >= 0 -> {:ok, value}
      _ -> {:error, :invalid_format}
    end
  end

  # Path builders

  @doc """
  Builds the cluster state path.

  ## Examples

      iex> Keys.cluster_state_path()
      "state"
  """
  @spec cluster_state_path() :: String.t()
  def cluster_state_path do
    "state"
  end

  @doc """
  Builds a chunk path for a shard.

  ## Examples

      iex> Keys.chunk_path("a", 1000)
      "c/a/3w5e11264sg0n"
  """
  @spec chunk_path(String.t(), non_neg_integer()) :: String.t()
  def chunk_path(shard_tag, highest_version) when is_binary(shard_tag) do
    "c/#{shard_tag}/#{version_to_key(highest_version)}"
  end

  @doc """
  Builds a chunks prefix for listing all chunks in a shard.

  ## Examples

      iex> Keys.chunks_prefix("a")
      "c/a/"
  """
  @spec chunks_prefix(String.t()) :: String.t()
  def chunks_prefix(shard_tag) when is_binary(shard_tag) do
    "c/#{shard_tag}/"
  end

  @doc """
  Builds a snapshot path for a shard.

  ## Examples

      iex> Keys.snapshot_path("a", 1000)
      "s/a/3w5e11264sg0n"
  """
  @spec snapshot_path(String.t(), non_neg_integer()) :: String.t()
  def snapshot_path(shard_tag, version) when is_binary(shard_tag) do
    "s/#{shard_tag}/#{version_to_key(version)}"
  end

  @doc """
  Builds a snapshots prefix for listing all snapshots in a shard.

  ## Examples

      iex> Keys.snapshots_prefix("a")
      "s/a/"
  """
  @spec snapshots_prefix(String.t()) :: String.t()
  def snapshots_prefix(shard_tag) when is_binary(shard_tag) do
    "s/#{shard_tag}/"
  end

  @doc """
  Extracts the version from a chunk or snapshot key.

  ## Examples

      iex> Keys.extract_version("c/a/3w5e11264sg0n")
      {:ok, 1000}

      iex> Keys.extract_version("invalid!")
      {:error, :invalid_format}
  """
  @spec extract_version(String.t()) :: {:ok, non_neg_integer()} | {:error, :invalid_format}
  def extract_version(key) when is_binary(key) do
    key
    |> Path.basename()
    |> key_to_version()
  end
end
