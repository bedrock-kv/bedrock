defmodule Bedrock do
  @moduledoc """
  Core types and utilities for Bedrock, a distributed key-value store.

  This module defines the fundamental types used throughout the Bedrock system,
  including keys, values, versions, and time-related constructs for MVCC
  transaction processing.
  """

  alias Bedrock.Internal.Time.Interval

  @type key :: binary()
  @type key_range :: {min_inclusive :: key(), max_exclusive :: key()}
  @type value :: binary()
  @type key_value :: {key(), value()}

  @type version :: Bedrock.DataPlane.Version.t()
  @type version_vector :: {oldest :: version(), newest :: version()}

  @type transaction :: binary()

  @type transaction_map :: %{
          optional(:mutations) => [mutation()] | nil,
          optional(:write_conflicts) => [key_range()] | nil,
          optional(:read_conflicts) => {version(), [key_range()]} | nil,
          optional(:commit_version) => version() | nil,
          optional(:shard_index) => [{non_neg_integer(), non_neg_integer()}] | nil
        }

  @type mutation ::
          {:set, key(), value()}
          | {:clear, key()}
          | {:clear_range, key(), key()}
          | {:atomic, :add, key(), value()}
          | {:atomic, :min, key(), value()}
          | {:atomic, :max, key(), value()}
          | {:atomic, :bit_and, key(), value()}
          | {:atomic, :bit_or, key(), value()}
          | {:atomic, :bit_xor, key(), value()}
          | {:atomic, :byte_min, key(), value()}
          | {:atomic, :byte_max, key(), value()}
          | {:atomic, :append_if_fits, key(), value()}
          | {:atomic, :compare_and_clear, key(), value()}

  @type epoch :: non_neg_integer()
  @type quorum :: pos_integer()
  @type timeout_in_ms :: :infinity | non_neg_integer()
  @type timestamp_in_ms :: integer()

  @type interval_in_ms :: :infinity | non_neg_integer()
  @type interval_in_us :: :infinity | non_neg_integer()

  @type time_unit :: Interval.unit()
  @type interval :: {Bedrock.time_unit(), non_neg_integer()}

  @type range_tag :: non_neg_integer()

  @type service :: :coordination | :log | :storage
  @type service_id :: String.t()
  @type lock_token :: binary()

  @doc """
  The end-of-keyspace sentinel value.

  This binary value is lexicographically greater than any valid key and is used
  to represent unbounded key ranges. For ergonomics, the atom `:end` is also
  accepted in the public API and is automatically converted to this sentinel.
  """
  @end_of_keyspace <<0xFF, 0xFF>>
  def end_of_keyspace, do: @end_of_keyspace

  @doc """
  Creates a key range from a minimum inclusive key to a maximum exclusive key.

  For convenience, the atom `:end` is accepted as `max_key_exclusive` to represent
  an unbounded range, and is automatically converted to the end-of-keyspace sentinel.

  ## Parameters

    - `min_key`: The minimum key value (inclusive).
    - `max_key_exclusive`: The maximum key value (exclusive), or `:end` for unbounded.

  ## Returns

    - A tuple representing the key range.

  ## Examples

      iex> Bedrock.key_range("a", "z")
      {"a", "z"}

      iex> Bedrock.key_range("a", :end)
      {"a", <<0xFF, 0xFF>>}

  """
  @spec key_range(Bedrock.key(), Bedrock.key() | :end) :: Bedrock.key_range()
  def key_range(min_key, :end) when min_key < @end_of_keyspace, do: {min_key, @end_of_keyspace}
  def key_range(min_key, max_key_exclusive) when min_key < max_key_exclusive, do: {min_key, max_key_exclusive}
  def key_range(_min_key, _max_key_exclusive), do: raise(ArgumentError, "min_key must be less than max_key_exclusive")
end
