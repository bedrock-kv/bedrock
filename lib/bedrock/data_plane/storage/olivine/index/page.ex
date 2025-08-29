defmodule Bedrock.DataPlane.Storage.Olivine.Index.Page do
  @moduledoc """
  Page management routines for the Olivine storage driver.

  This module handles all page-related operations including:
  - Page creation, encoding, and decoding
  - Page splitting and key management
  - Tree operations for page lookup
  - Page chain walking and range queries
  """

  @type id :: non_neg_integer()
  @type page_map :: %{
          id: id(),
          next_id: id(),
          key_versions: [{binary(), Bedrock.version()}]
        }
  @type t :: binary() | page_map()
  @type operation :: {:set, Bedrock.version()} | :clear

  @doc """
  Creates a page with key-version tuples and optional next_id.
  Returns the binary-encoded page.
  """
  @spec new(id :: id(), key_versions :: [{binary(), Bedrock.version()}], next_id :: id()) :: binary()
  def new(id, key_versions, next_id \\ 0) when is_list(key_versions) do
    versions = Enum.map(key_versions, fn {_key, version} -> version end)

    if !Enum.all?(versions, &(is_binary(&1) and byte_size(&1) == 8)) do
      raise ArgumentError, "All versions must be 8-byte binary values"
    end

    # Use direct encoding for maximum efficiency
    encode_page_direct(id, next_id || 0, key_versions)
  end

  @spec from_map(t()) :: binary()
  def from_map(%{key_versions: key_versions, id: id, next_id: next_id}),
    do: encode_page_direct(id, next_id, key_versions)

  def from_map(binary) when is_binary(binary), do: binary

  # Direct page encoding using the efficient binary approach
  @spec encode_page_direct(id(), id(), [{binary(), Bedrock.version()}]) :: binary()
  defp encode_page_direct(id, next_id, key_versions) do
    key_count = length(key_versions)

    # Encode entries directly as iodata
    {new_payload, right_key} = encode_entries(key_versions)
    right_key_offset = calculate_right_key_offset(new_payload, right_key)

    header = <<
      id::unsigned-big-32,
      next_id::unsigned-big-32,
      key_count::unsigned-big-16,
      right_key_offset::unsigned-big-32,
      0::unsigned-big-16
    >>

    # Single conversion from iodata to binary
    IO.iodata_to_binary([header | new_payload])
  end

  # Encode key-version pairs directly as iodata
  @spec encode_entries([{binary(), Bedrock.version()}]) :: {iodata(), integer() | nil}
  defp encode_entries([]), do: {[], nil}
  defp encode_entries(key_versions), do: encode_entries(key_versions, [], nil)

  @spec encode_entries([{binary(), Bedrock.version()}], iodata(), binary() | nil) :: {iodata(), binary()}
  defp encode_entries([], acc, right_key), do: {Enum.reverse(acc), right_key}

  defp encode_entries([{right_key, version} | rest], acc, _right_key),
    do: encode_entries(rest, [encode_version_key_entry(version, right_key) | acc], right_key)

  @doc """
  Applies a map of operations to a binary page, returning the updated binary page.
  Operations can be {:set, version} or :clear.
  """
  @spec apply_operations(binary(), %{Bedrock.key() => operation()}) :: binary()
  def apply_operations(page, operations) when map_size(operations) == 0, do: page

  def apply_operations(page, operations) when is_binary(page) do
    <<
      id::unsigned-big-32,
      next_id::unsigned-big-32,
      key_count::unsigned-big-16,
      _right_key_offset::unsigned-big-32,
      _padding::unsigned-big-16,
      payload::binary
    >> = page

    {parts, key_count_delta, right_key} =
      do_apply_operations(payload, Enum.sort_by(operations, &elem(&1, 0)), 16, [16], 0, nil)

    new_payload = convert_parts_to_iodata(parts, page)
    new_right_key_offset = calculate_right_key_offset(new_payload, right_key)

    IO.iodata_to_binary([
      <<
        id::unsigned-big-32,
        next_id::unsigned-big-32,
        key_count + key_count_delta::unsigned-big-16,
        new_right_key_offset::unsigned-big-32,
        0::unsigned-big-16
      >>,
      new_payload
    ])
  end

  defp calculate_right_key_offset(_payload, nil), do: 0
  defp calculate_right_key_offset(payload, right_key), do: 16 + IO.iodata_length(payload) - byte_size(right_key) - 2

  defp convert_parts_to_iodata(parts, page, acc \\ [])

  defp convert_parts_to_iodata([part | rest], page, acc) do
    binary_part =
      case part do
        start_offset when is_integer(start_offset) ->
          binary_part(page, start_offset, byte_size(page) - start_offset)

        {start_offset, length} ->
          binary_part(page, start_offset, length)

        binary when is_binary(binary) ->
          binary
      end

    convert_parts_to_iodata(rest, page, [binary_part | acc])
  end

  defp convert_parts_to_iodata([], _page, acc), do: acc

  @spec do_apply_operations(
          binary(),
          [{binary(), operation()}],
          non_neg_integer(),
          list(),
          integer(),
          binary() | nil
        ) :: {list(), integer(), binary() | nil}
  defp do_apply_operations(
         <<_version::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>> = entries,
         operations,
         current_offset,
         acc,
         key_count_delta,
         right_key
       ) do
    entry_size = 8 + 2 + key_len
    next_offset = current_offset + entry_size

    case operations do
      [{op_key, op} | remaining_ops] when op_key < key ->
        case op do
          :clear ->
            do_apply_operations(entries, remaining_ops, current_offset, acc, key_count_delta, right_key)

          {:set, version} ->
            do_apply_operations(
              entries,
              remaining_ops,
              current_offset,
              [current_offset | [encode_version_key_entry(version, op_key) | slice_or_pop(acc, current_offset)]],
              key_count_delta + 1,
              max_of(right_key, op_key)
            )
        end

      [{^key, op} | remaining_ops] ->
        case op do
          :clear ->
            do_apply_operations(
              rest,
              remaining_ops,
              next_offset,
              [next_offset | slice_or_pop(acc, current_offset)],
              key_count_delta - 1,
              right_key
            )

          {:set, version} ->
            do_apply_operations(
              rest,
              remaining_ops,
              next_offset,
              [next_offset, encode_version_key_entry(version, key) | slice_or_pop(acc, current_offset)],
              key_count_delta,
              max_of(right_key, key)
            )
        end

      _ ->
        new_largest = max_of(right_key, key)
        do_apply_operations(rest, operations, next_offset, acc, key_count_delta, new_largest)
    end
  end

  defp do_apply_operations(_entries, [], _current_offset, acc, key_count_delta, right_key),
    do: {acc, key_count_delta, right_key}

  # All remaining operations must be greater than the current last_key
  defp do_apply_operations(<<>>, operations, current_offset, acc, key_count_delta, right_key),
    do: add_remaining_operations_as_parts(operations, slice_or_pop(acc, current_offset), key_count_delta, right_key)

  # Simple max helper - cleaner than case statements
  defp max_of(nil, key), do: key
  defp max_of(key, nil), do: key
  defp max_of(key1, key2) when key1 >= key2, do: key1
  defp max_of(_key1, key2), do: key2

  # Add remaining operations as new entries
  @spec add_remaining_operations_as_parts(
          [{binary(), operation()}],
          list(),
          integer(),
          binary() | nil
        ) :: {list(), integer(), binary() | nil}
  defp add_remaining_operations_as_parts([{_key, :clear} | tail], acc, key_count_delta, right_key),
    do: add_remaining_operations_as_parts(tail, acc, key_count_delta, right_key)

  defp add_remaining_operations_as_parts([{key, {:set, version}} | tail], acc, key_count_delta, right_key) do
    new_largest = max_of(right_key, key)

    add_remaining_operations_as_parts(
      tail,
      [encode_version_key_entry(version, key) | acc],
      key_count_delta + 1,
      new_largest
    )
  end

  defp add_remaining_operations_as_parts([], acc, key_count_delta, right_key), do: {acc, key_count_delta, right_key}

  @spec encode_version_key_entry(Bedrock.version(), binary()) :: binary()
  defp encode_version_key_entry(version, key) when is_binary(version) and byte_size(version) == 8,
    do: <<version::binary-size(8), byte_size(key)::unsigned-big-16, key::binary>>

  # If the accumulator _is_ the current_offset, pop it, otherwise we build a slice.
  defp slice_or_pop(acc, current_offset) do
    case acc do
      [^current_offset | tail] -> tail
      [slice_start | tail] when is_integer(slice_start) -> [{slice_start, current_offset - slice_start} | tail]
    end
  end

  @doc """
  Validates that a binary has the correct page format and valid last_key offset.
  Returns :ok if valid, {:error, :corrupted_page} if invalid.
  """
  @spec validate(binary()) :: :ok | {:error, :corrupted_page}
  def validate(<<_id::32, _next_id::32, 0::16, _right_key_offset::32, _reserved::16, _entries::binary>>), do: :ok

  def validate(
        <<_id::32, _next_id::32, _key_count::16, right_key_offset::32, _reserved::16,
          _data::binary-size(right_key_offset - 16), _key_len::16, _key::binary>>
      ),
      do: :ok

  def validate(_), do: {:error, :corrupted_page}

  @spec to_map(binary()) :: {:ok, t()} | {:error, :invalid_page}
  def to_map(
        <<id::unsigned-big-32, next_id::unsigned-big-32, key_count::unsigned-big-16, _right_key_offset::unsigned-big-32,
          _reserved::unsigned-big-16, entries_data::binary>>
      ) do
    entries_data
    |> decode_entries(key_count, [])
    |> case do
      {:ok, key_versions} ->
        {:ok,
         %{
           id: id,
           next_id: next_id,
           key_versions: key_versions
         }}

      error ->
        error
    end
  end

  def to_map(_), do: {:error, :invalid_page}

  # Decode interleaved version-key entries
  @spec decode_entries(binary(), non_neg_integer(), [{binary(), Bedrock.version()}]) ::
          {:ok, [{binary(), Bedrock.version()}]} | {:error, :invalid_entries}
  defp decode_entries(
         <<version::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>>,
         count,
         acc
       ) do
    decode_entries(rest, count - 1, [{key, version} | acc])
  end

  defp decode_entries(<<>>, 0, acc), do: {:ok, Enum.reverse(acc)}
  defp decode_entries(_, _, _), do: {:error, :invalid_entries}

  @spec id(t()) :: id()
  def id(<<page_id::unsigned-big-32, _rest::binary>>), do: page_id
  def id(%{id: page_id}), do: page_id

  @spec next_id(t()) :: id()
  def next_id(<<_page_id::unsigned-big-32, next_page_id::unsigned-big-32, _rest::binary>>), do: next_page_id
  def next_id(%{next_id: next_page_id}), do: next_page_id

  @spec key_versions(t()) :: [{binary(), Bedrock.version()}]
  def key_versions(
        <<_page_id::unsigned-big-32, _next_id::unsigned-big-32, key_count::unsigned-big-16,
          _right_key_offset::unsigned-big-32, _reserved::unsigned-big-16, entries_data::binary>>
      ) do
    {:ok, key_versions} = decode_entries(entries_data, key_count, [])
    key_versions
  end

  def key_versions(%{key_versions: kvs}), do: kvs

  @spec key_count(t()) :: non_neg_integer()
  def key_count(<<_page_id::unsigned-big-32, _next_id::unsigned-big-32, key_count::unsigned-big-16, _rest::binary>>),
    do: key_count

  def key_count(%{key_versions: kvs}), do: length(kvs)

  @spec keys(t()) :: [binary()]
  def keys(page) when is_binary(page) do
    page |> key_versions() |> Enum.map(fn {key, _version} -> key end)
  end

  @spec extract_keys(t()) :: [binary()]
  def extract_keys(page), do: keys(page)

  @spec left_key(t()) :: binary() | nil
  def left_key(<<_page_id::unsigned-big-32, _next_id::unsigned-big-32, 0::unsigned-big-16, _rest::binary>>), do: nil

  def left_key(
        <<_page_id::unsigned-big-32, _next_id::unsigned-big-32, _key_count::unsigned-big-16,
          _right_key_offset::unsigned-big-32, _reserved::unsigned-big-16, _version::binary-size(8),
          key_len::unsigned-big-16, key::binary-size(key_len), _rest::binary>>
      ) do
    key
  end

  @spec right_key(t()) :: binary() | nil
  # O(1) access using right_key_offset - read from offset to end of binary
  def right_key(<<_page_id::unsigned-big-32, _next_id::unsigned-big-32, 0::unsigned-big-16, _rest::binary>>), do: nil

  def right_key(
        <<_page_id::unsigned-big-32, _next_id::unsigned-big-32, _key_count::unsigned-big-16,
          right_key_offset::unsigned-big-32, _reserved::unsigned-big-16, _data::binary-size(right_key_offset - 16),
          key_len::unsigned-big-16, key::binary-size(key_len)>>
      ) do
    key
  end

  @spec version_for_key(t(), Bedrock.key()) :: {:ok, Bedrock.version()} | {:error, :not_found}
  def version_for_key(
        <<_page_id::unsigned-big-32, _next_id::unsigned-big-32, key_count::unsigned-big-16,
          _right_key_offset::unsigned-big-32, _reserved::unsigned-big-16, entries_data::binary>>,
        target_key
      ),
      do: search_entries_for_key(entries_data, key_count, target_key)

  @spec search_entries_for_key(binary(), non_neg_integer(), Bedrock.key()) ::
          {:ok, Bedrock.version()} | {:error, :not_found}
  defp search_entries_for_key(_data, 0, _target_key), do: {:error, :not_found}

  defp search_entries_for_key(
         <<version::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>>,
         count,
         target_key
       ) do
    if key == target_key do
      {:ok, version}
    else
      search_entries_for_key(rest, count - 1, target_key)
    end
  end

  defp search_entries_for_key(_, _, _), do: :not_found

  # Alias for backward compatibility with tests

  @spec stream_key_versions_in_range(Enumerable.t(t()), Bedrock.key(), Bedrock.key()) ::
          Enumerable.t({Bedrock.key(), Bedrock.version()})
  def stream_key_versions_in_range(pages_stream, start_key, end_key) do
    pages_stream
    |> Stream.flat_map(fn page -> key_versions(page) end)
    |> Stream.drop_while(fn {key, _version} -> key < start_key end)
    |> Stream.take_while(fn {key, _version} -> key < end_key end)
  end

  @doc """
  Checks if a page contains a specific key.
  """
  @spec has_key?(t() | binary(), Bedrock.key()) :: boolean()
  def has_key?(page, key) when is_binary(page) do
    case version_for_key(page, key) do
      {:ok, _version} -> true
      _ -> false
    end
  end

  @doc """
  Checks if a page is empty (has no keys).
  """
  @spec empty?(t() | binary()) :: boolean()
  def empty?(page), do: key_count(page) == 0

  @doc """
  Checks if two pages are the same page (same ID).
  """
  @spec same_page?(t() | binary(), t() | binary()) :: boolean()
  def same_page?(page1, page2), do: id(page1) == id(page2)

  @doc """
  Checks if a page has the given ID.
  """
  @spec has_id?(t() | binary(), id()) :: boolean()
  def has_id?(page, page_id), do: id(page) == page_id

  @doc """
  Splits a page at the given key offset, creating two new pages.
  Returns {left_page, right_page} where left_page keeps the original ID
  and right_page gets the new_page_id.
  """
  @spec split_page(binary(), non_neg_integer(), id()) :: {binary(), binary()}
  def split_page(page, key_offset, new_page_id) do
    <<page_id::32, next_id::32, _key_count::16, _right_key_offset::32, _reserved::16, _entries::binary>> = page

    # Get all key-version pairs
    key_versions = key_versions(page)

    # Split at the specified offset
    {left_key_versions, right_key_versions} = Enum.split(key_versions, key_offset)

    # Create two new pages
    left_page = new(page_id, left_key_versions, new_page_id)
    right_page = new(new_page_id, right_key_versions, next_id)

    {left_page, right_page}
  end
end
