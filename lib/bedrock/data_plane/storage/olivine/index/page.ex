defmodule Bedrock.DataPlane.Storage.Olivine.Index.Page do
  @moduledoc """
  Page management routines for the Olivine storage driver.

  Pages are the fundamental storage unit in the Olivine B-tree index. Each page
  contains sorted key-version pairs and chain pointers for traversal.

  ## Key Features

  - **Binary encoding**: Pages are stored as optimized binary data for efficiency
  - **Chain linking**: Pages form a linked list via `next_id` pointers
  - **Sorted keys**: Keys within each page are maintained in ascending order
  - **Version tracking**: Each key has an associated version for MVCC support
  - **Size limits**: Pages are split when they exceed 256 keys to maintain performance

  ## Operations

  - Page creation, encoding, and decoding
  - Page splitting for size management
  - Key range queries within pages
  - Chain traversal for ordered iteration
  - Validation of page structure and invariants
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

    encode_page_direct(id, next_id || 0, key_versions)
  end

  @spec encode_page_direct(id(), id(), [{binary(), Bedrock.version()}]) :: binary()
  defp encode_page_direct(id, next_id, key_versions) do
    key_count = length(key_versions)

    {payload_iodata, rightmost_key} = encode_entries_as_iodata(key_versions)
    rightmost_key_offset = calculate_rightmost_key_offset(payload_iodata, rightmost_key)

    header = <<
      id::unsigned-big-32,
      next_id::unsigned-big-32,
      key_count::unsigned-big-16,
      rightmost_key_offset::unsigned-big-32,
      0::unsigned-big-16
    >>

    IO.iodata_to_binary([header | payload_iodata])
  end

  @spec encode_entries_as_iodata([{binary(), Bedrock.version()}]) :: {iodata(), integer() | nil}
  defp encode_entries_as_iodata([]), do: {[], nil}
  defp encode_entries_as_iodata(key_versions), do: encode_entries_as_iodata(key_versions, [], nil)

  @spec encode_entries_as_iodata([{binary(), Bedrock.version()}], iodata(), binary() | nil) :: {iodata(), binary()}
  defp encode_entries_as_iodata([], accumulated_iodata, rightmost_key),
    do: {Enum.reverse(accumulated_iodata), rightmost_key}

  defp encode_entries_as_iodata([{current_key, version} | remaining_entries], accumulated_iodata, _previous_rightmost),
    do:
      encode_entries_as_iodata(
        remaining_entries,
        [encode_version_key_entry(version, current_key) | accumulated_iodata],
        current_key
      )

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

    {binary_segments, key_count_delta, rightmost_key} =
      do_apply_operations(payload, Enum.sort_by(operations, &elem(&1, 0)), 16, [16], 0, nil)

    new_payload = convert_binary_segments_to_iodata(binary_segments, page)
    new_rightmost_key_offset = calculate_rightmost_key_offset(new_payload, rightmost_key)

    IO.iodata_to_binary([
      <<
        id::unsigned-big-32,
        next_id::unsigned-big-32,
        key_count + key_count_delta::unsigned-big-16,
        new_rightmost_key_offset::unsigned-big-32,
        0::unsigned-big-16
      >>,
      new_payload
    ])
  end

  defp calculate_rightmost_key_offset(_payload, nil), do: 0

  defp calculate_rightmost_key_offset(payload, rightmost_key),
    do: 16 + IO.iodata_length(payload) - byte_size(rightmost_key) - 2

  defp convert_binary_segments_to_iodata(segments, page, accumulated_iodata \\ [])

  defp convert_binary_segments_to_iodata([segment | remaining_segments], page, accumulated_iodata) do
    binary_segment =
      case segment do
        start_offset when is_integer(start_offset) ->
          binary_part(page, start_offset, byte_size(page) - start_offset)

        {start_offset, length} ->
          binary_part(page, start_offset, length)

        binary when is_binary(binary) ->
          binary
      end

    convert_binary_segments_to_iodata(remaining_segments, page, [binary_segment | accumulated_iodata])
  end

  defp convert_binary_segments_to_iodata([], _page, accumulated_iodata), do: accumulated_iodata

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
         rightmost_key
       ) do
    entry_size = 8 + 2 + key_len
    next_offset = current_offset + entry_size

    case operations do
      [{op_key, op} | remaining_ops] when op_key < key ->
        case op do
          :clear ->
            do_apply_operations(entries, remaining_ops, current_offset, acc, key_count_delta, rightmost_key)

          {:set, version} ->
            do_apply_operations(
              entries,
              remaining_ops,
              current_offset,
              [current_offset | [encode_version_key_entry(version, op_key) | slice_or_pop(acc, current_offset)]],
              key_count_delta + 1,
              rightmost_of(rightmost_key, op_key)
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
              rightmost_key
            )

          {:set, version} ->
            do_apply_operations(
              rest,
              remaining_ops,
              next_offset,
              [next_offset, encode_version_key_entry(version, key) | slice_or_pop(acc, current_offset)],
              key_count_delta,
              rightmost_of(rightmost_key, key)
            )
        end

      _ ->
        new_rightmost = rightmost_of(rightmost_key, key)
        do_apply_operations(rest, operations, next_offset, acc, key_count_delta, new_rightmost)
    end
  end

  defp do_apply_operations(_entries, [], _current_offset, acc, key_count_delta, rightmost_key),
    do: {acc, key_count_delta, rightmost_key}

  defp do_apply_operations(<<>>, operations, current_offset, acc, key_count_delta, rightmost_key),
    do:
      add_remaining_operations_as_binary_segments(
        operations,
        slice_or_pop(acc, current_offset),
        key_count_delta,
        rightmost_key
      )

  defp rightmost_of(nil, key), do: key
  defp rightmost_of(key, nil), do: key
  defp rightmost_of(key1, key2) when key1 >= key2, do: key1
  defp rightmost_of(_key1, key2), do: key2

  @spec add_remaining_operations_as_binary_segments(
          [{binary(), operation()}],
          list(),
          integer(),
          binary() | nil
        ) :: {list(), integer(), binary() | nil}
  defp add_remaining_operations_as_binary_segments(
         [{_key, :clear} | remaining_ops],
         acc,
         key_count_delta,
         rightmost_key
       ),
       do: add_remaining_operations_as_binary_segments(remaining_ops, acc, key_count_delta, rightmost_key)

  defp add_remaining_operations_as_binary_segments(
         [{key, {:set, version}} | remaining_ops],
         acc,
         key_count_delta,
         rightmost_key
       ) do
    new_rightmost = rightmost_of(rightmost_key, key)

    add_remaining_operations_as_binary_segments(
      remaining_ops,
      [encode_version_key_entry(version, key) | acc],
      key_count_delta + 1,
      new_rightmost
    )
  end

  defp add_remaining_operations_as_binary_segments([], acc, key_count_delta, rightmost_key),
    do: {acc, key_count_delta, rightmost_key}

  @spec encode_version_key_entry(Bedrock.version(), binary()) :: binary()
  defp encode_version_key_entry(version, key) when is_binary(version) and byte_size(version) == 8,
    do: <<version::binary-size(8), byte_size(key)::unsigned-big-16, key::binary>>

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

  @spec search_entries_with_position(binary(), non_neg_integer(), Bedrock.key()) ::
          {:found, pos :: non_neg_integer()} | {:not_found, insertion_point :: non_neg_integer()}
  def search_entries_with_position(data, count, target_key),
    do: search_entries_with_position(data, count, target_key, 0)

  defp search_entries_with_position(_data, 0, _target_key, pos), do: {:not_found, pos}

  defp search_entries_with_position(
         <<_version::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>>,
         count,
         target_key,
         pos
       ) do
    cond do
      key == target_key -> {:found, pos}
      key > target_key -> {:not_found, pos}
      true -> search_entries_with_position(rest, count - 1, target_key, pos + 1)
    end
  end

  defp search_entries_with_position(_, _, _, pos), do: {:not_found, pos}

  @spec decode_entry_at_position(binary(), non_neg_integer(), non_neg_integer()) ::
          {:ok, {key :: binary(), version :: binary()}} | :out_of_bounds
  def decode_entry_at_position(_entries_data, position, key_count) when position >= key_count, do: :out_of_bounds

  def decode_entry_at_position(entries_data, 0, _key_count) do
    <<version::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len), _rest::binary>> = entries_data
    {:ok, {key, version}}
  end

  def decode_entry_at_position(
        <<_version::binary-size(8), key_len::unsigned-big-16, _key::binary-size(key_len), rest::binary>>,
        position,
        key_count
      ) do
    decode_entry_at_position(rest, position - 1, key_count)
  end

  def decode_entry_at_position(_, _, _), do: :out_of_bounds

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
  Splits a page at the given key offset, creating two new pages.
  Returns {left_page, right_page} where left_page keeps the original ID
  and right_page gets the new_page_id.
  """
  @spec split_page(binary(), non_neg_integer(), id()) :: {binary(), binary()}
  def split_page(page, key_offset, new_page_id) do
    <<page_id::32, next_id::32, _key_count::16, _right_key_offset::32, _reserved::16, _entries::binary>> = page

    key_versions = key_versions(page)

    {left_key_versions, right_key_versions} = Enum.split(key_versions, key_offset)

    left_page = new(page_id, left_key_versions, new_page_id)
    right_page = new(new_page_id, right_key_versions, next_id)

    {left_page, right_page}
  end
end
