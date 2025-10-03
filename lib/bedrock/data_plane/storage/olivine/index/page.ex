defmodule Bedrock.DataPlane.Storage.Olivine.Index.Page do
  @moduledoc """
  Page management routines for the Olivine storage driver.

  Pages are the fundamental storage unit in the Olivine B-tree index. Each page
  contains sorted key-locator pairs and chain pointers for traversal.

  ## Key Features

  - **Binary encoding**: Pages are stored as optimized binary data for efficiency
  - **Chain linking**: Pages form a linked list via `next_id` pointers
  - **Sorted keys**: Keys within each page are maintained in ascending order
  - **Version tracking**: Each key has an associated locator for MVCC support
  - **Size limits**: Pages are split when they exceed 256 keys to maintain performance

  ## Operations

  - Page creation, encoding, and decoding
  - Page splitting for size management
  - Key range queries within pages
  - Chain traversal for ordered iteration
  - Validation of page structure and invariants
  """

  alias Bedrock.DataPlane.Storage.Olivine.Database

  @type id :: non_neg_integer()
  @type page_map :: %{
          id: id(),
          next_id: id(),
          key_locators: [{binary(), Database.locator()}]
        }
  @type t :: binary() | page_map()
  @type operation :: {:set, Database.locator()} | :clear

  @doc """
  Creates a page with key-locator tuples.
  Returns the binary-encoded page with next_id set to 0.
  """
  @spec new(id :: id(), key_locators :: [{binary(), Database.locator()}]) :: binary()
  def new(id, key_locators) when is_list(key_locators) do
    locators = Enum.map(key_locators, fn {_key, locator} -> locator end)

    if !Enum.all?(locators, &(is_binary(&1) and byte_size(&1) == 8)) do
      raise ArgumentError, "All locators must be 8-byte binary values"
    end

    encode_page_direct(id, key_locators)
  end

  @spec encode_page_direct(id(), [{binary(), Database.locator()}]) :: binary()
  defp encode_page_direct(id, key_locators) do
    key_count = length(key_locators)
    {payload_iodata, rightmost_key} = encode_entries_as_iodata(key_locators)
    rightmost_key_offset_from_end = calculate_rightmost_key_offset(rightmost_key)

    header = <<
      id::unsigned-big-32,
      key_count::unsigned-big-16,
      rightmost_key_offset_from_end::unsigned-big-32,
      0::unsigned-big-48
    >>

    IO.iodata_to_binary([header | payload_iodata])
  end

  @spec encode_entries_as_iodata([{binary(), Database.locator()}]) :: {iodata(), binary() | nil}
  defp encode_entries_as_iodata([]), do: {[], nil}
  defp encode_entries_as_iodata(key_locators), do: encode_entries_as_iodata(key_locators, [], nil)

  @spec encode_entries_as_iodata([{binary(), Database.locator()}], iodata(), binary() | nil) :: {iodata(), binary()}
  defp encode_entries_as_iodata([], accumulated_iodata, rightmost_key),
    do: {Enum.reverse(accumulated_iodata), rightmost_key}

  defp encode_entries_as_iodata([{current_key, locator} | remaining_entries], accumulated_iodata, _previous_rightmost),
    do:
      encode_entries_as_iodata(
        remaining_entries,
        [encode_locator_key_entry(locator, current_key) | accumulated_iodata],
        current_key
      )

  @doc """
  Applies operations to a binary page, returning per-key segments for efficient splitting.

  Returns a tuple of:
  - page_id: The page ID from the header
  - segments: Reversed list of per-key segments (each segment is one {key, locator} entry)
    - `{offset, length}` for unchanged keys (reference to old binary)
    - `<<binary>>` for new/modified keys
  - key_count: Final number of keys after operations
  - rightmost_key: The rightmost key in the page (or nil if empty)

  The segments list is in reverse order (rightmost key first), which is natural
  from the merge algorithm and gives us the rightmost key for free.
  """
  @type segment :: {non_neg_integer(), non_neg_integer()} | binary()

  @spec apply_operations_as_segments(binary(), %{Bedrock.key() => operation()}) ::
          {[segment()], key_count :: non_neg_integer(), rightmost_key :: binary() | nil}
  def apply_operations_as_segments(page, operations) when map_size(operations) == 0 do
    <<_id::unsigned-big-32, key_count::unsigned-big-16, _rest::binary>> = page

    if key_count == 0 do
      {[], 0, nil}
    else
      <<_header::binary-size(16), payload::binary>> = page
      segments = build_segments_from_existing(payload, 16, [])
      rightmost = right_key(page)
      {segments, key_count, rightmost}
    end
  end

  def apply_operations_as_segments(page, operations) when is_binary(page) do
    <<
      _id::unsigned-big-32,
      key_count::unsigned-big-16,
      _right_key_offset::unsigned-big-32,
      _padding::unsigned-big-48,
      payload::binary
    >> = page

    {segments, key_count_delta, rightmost_key} =
      do_apply_operations_per_key(payload, Enum.sort_by(operations, &elem(&1, 0)), 16, [], 0, nil)

    {segments, key_count + key_count_delta, rightmost_key}
  end

  @doc """
  Builds a binary page from iodata (already materialized segments).

  Takes a page ID, iodata payload, key count, and rightmost key.
  This is used when segments have already been materialized by `take_and_materialize`.

  Returns a complete binary page with proper header and payload.
  """
  @spec build_from_segments_iodata(id(), iodata(), non_neg_integer(), binary() | nil) :: binary()
  def build_from_segments_iodata(page_id, iodata, key_count, rightmost_key) do
    if key_count == 0 do
      <<
        page_id::unsigned-big-32,
        0::unsigned-big-16,
        0::unsigned-big-32,
        0::unsigned-big-48
      >>
    else
      rightmost_key_offset_from_end = calculate_rightmost_key_offset(rightmost_key)

      IO.iodata_to_binary([
        <<
          page_id::unsigned-big-32,
          key_count::unsigned-big-16,
          rightmost_key_offset_from_end::unsigned-big-32,
          0::unsigned-big-48
        >>,
        iodata
      ])
    end
  end

  @doc """
  Builds a binary page from pre-computed segments.

  Takes a page ID, list of segments (in reverse order), final key count, original page,
  and the rightmost key. The segments list and rightmost key should come from
  `apply_operations_as_segments/2`.

  Returns a complete binary page with proper header and payload.
  """
  @spec build_from_segments(id(), [segment()], non_neg_integer(), binary(), binary() | nil) :: binary()
  def build_from_segments(page_id, segments, key_count, original_page, rightmost_key) do
    if key_count == 0 do
      <<
        page_id::unsigned-big-32,
        0::unsigned-big-16,
        0::unsigned-big-32,
        0::unsigned-big-48
      >>
    else
      new_payload = convert_binary_segments_to_iodata(segments, original_page)
      rightmost_key_offset_from_end = calculate_rightmost_key_offset(rightmost_key)

      IO.iodata_to_binary([
        <<
          page_id::unsigned-big-32,
          key_count::unsigned-big-16,
          rightmost_key_offset_from_end::unsigned-big-32,
          0::unsigned-big-48
        >>,
        new_payload
      ])
    end
  end

  @doc """
  Applies a map of operations to a binary page, returning the updated binary page.
  Operations can be {:set, locator} or :clear.
  """
  @spec apply_operations(binary(), %{Bedrock.key() => operation()}) :: binary()
  def apply_operations(page, operations) when map_size(operations) == 0, do: page

  def apply_operations(page, operations) when is_binary(page) do
    <<
      id::unsigned-big-32,
      key_count::unsigned-big-16,
      _right_key_offset::unsigned-big-32,
      _padding::unsigned-big-48,
      payload::binary
    >> = page

    {binary_segments, key_count_delta, rightmost_key} =
      do_apply_operations(payload, Enum.sort_by(operations, &elem(&1, 0)), 16, [16], 0, nil)

    new_payload = convert_binary_segments_to_iodata(binary_segments, page)
    rightmost_key_offset_from_end = calculate_rightmost_key_offset(rightmost_key)

    IO.iodata_to_binary([
      <<
        id::unsigned-big-32,
        key_count + key_count_delta::unsigned-big-16,
        rightmost_key_offset_from_end::unsigned-big-32,
        0::unsigned-big-48
      >>,
      new_payload
    ])
  end

  defp convert_binary_segments_to_iodata(segments, page, accumulated_iodata \\ [])

  defp convert_binary_segments_to_iodata([segment | remaining_segments], page, accumulated_iodata) do
    iodata_segment =
      case segment do
        start_offset when is_integer(start_offset) ->
          binary_part(page, start_offset, byte_size(page) - start_offset)

        {start_offset, length} ->
          binary_part(page, start_offset, length)

        binary when is_binary(binary) ->
          binary
      end

    convert_binary_segments_to_iodata(remaining_segments, page, [iodata_segment | accumulated_iodata])
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
         <<_locator::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>> = entries,
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

          {:set, locator} ->
            do_apply_operations(
              entries,
              remaining_ops,
              current_offset,
              [current_offset | [encode_locator_key_entry(locator, op_key) | slice_or_pop(acc, current_offset)]],
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

          {:set, locator} ->
            do_apply_operations(
              rest,
              remaining_ops,
              next_offset,
              [next_offset, encode_locator_key_entry(locator, key) | slice_or_pop(acc, current_offset)],
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
         [{key, {:set, locator}} | remaining_ops],
         acc,
         key_count_delta,
         rightmost_key
       ) do
    new_rightmost = rightmost_of(rightmost_key, key)

    add_remaining_operations_as_binary_segments(
      remaining_ops,
      [encode_locator_key_entry(locator, key) | acc],
      key_count_delta + 1,
      new_rightmost
    )
  end

  defp add_remaining_operations_as_binary_segments([], acc, key_count_delta, rightmost_key),
    do: {acc, key_count_delta, rightmost_key}

  @spec encode_locator_key_entry(Database.locator(), binary()) :: binary()
  defp encode_locator_key_entry(locator, key) when is_binary(locator) and byte_size(locator) == 8,
    do: <<locator::binary-size(8), byte_size(key)::unsigned-big-16, key::binary>>

  defp slice_or_pop(acc, current_offset) do
    case acc do
      [^current_offset | tail] -> tail
      [slice_start | tail] when is_integer(slice_start) -> [{slice_start, current_offset - slice_start} | tail]
    end
  end

  @spec do_apply_operations_per_key(
          binary(),
          [{binary(), operation()}],
          non_neg_integer(),
          list(),
          integer(),
          binary() | nil
        ) :: {[segment()], integer(), binary() | nil}
  defp do_apply_operations_per_key(
         <<_locator::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>> = entries,
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
            do_apply_operations_per_key(
              entries,
              remaining_ops,
              current_offset,
              acc,
              key_count_delta,
              rightmost_key
            )

          {:set, locator} ->
            new_entry = encode_locator_key_entry(locator, op_key)

            do_apply_operations_per_key(
              entries,
              remaining_ops,
              current_offset,
              [new_entry | acc],
              key_count_delta + 1,
              rightmost_of(rightmost_key, op_key)
            )
        end

      [{^key, op} | remaining_ops] ->
        case op do
          :clear ->
            do_apply_operations_per_key(
              rest,
              remaining_ops,
              next_offset,
              acc,
              key_count_delta - 1,
              rightmost_key
            )

          {:set, locator} ->
            new_entry = encode_locator_key_entry(locator, key)

            do_apply_operations_per_key(
              rest,
              remaining_ops,
              next_offset,
              [new_entry | acc],
              key_count_delta,
              rightmost_of(rightmost_key, key)
            )
        end

      _ ->
        new_rightmost = rightmost_of(rightmost_key, key)

        do_apply_operations_per_key(
          rest,
          operations,
          next_offset,
          [{current_offset, entry_size} | acc],
          key_count_delta,
          new_rightmost
        )
    end
  end

  defp do_apply_operations_per_key(_entries, [], _current_offset, acc, key_count_delta, rightmost_key),
    do: {acc, key_count_delta, rightmost_key}

  defp do_apply_operations_per_key(<<>>, operations, _current_offset, acc, key_count_delta, rightmost_key) do
    add_remaining_operations_per_key(operations, acc, key_count_delta, rightmost_key)
  end

  defp add_remaining_operations_per_key([], acc, key_count_delta, rightmost_key),
    do: {acc, key_count_delta, rightmost_key}

  defp add_remaining_operations_per_key([{_key, :clear} | remaining_ops], acc, key_count_delta, rightmost_key),
    do: add_remaining_operations_per_key(remaining_ops, acc, key_count_delta, rightmost_key)

  defp add_remaining_operations_per_key([{key, {:set, locator}} | remaining_ops], acc, key_count_delta, rightmost_key) do
    new_entry = encode_locator_key_entry(locator, key)

    add_remaining_operations_per_key(
      remaining_ops,
      [new_entry | acc],
      key_count_delta + 1,
      rightmost_of(rightmost_key, key)
    )
  end

  defp build_segments_from_existing(<<>>, _current_offset, acc), do: acc

  defp build_segments_from_existing(
         <<_locator::binary-size(8), key_len::unsigned-big-16, _key::binary-size(key_len), rest::binary>>,
         current_offset,
         acc
       ) do
    entry_size = 8 + 2 + key_len
    next_offset = current_offset + entry_size
    build_segments_from_existing(rest, next_offset, [{current_offset, entry_size} | acc])
  end

  @spec decode_entries(binary(), non_neg_integer(), [{binary(), Database.locator()}]) ::
          {:ok, [{binary(), Database.locator()}]} | {:error, :invalid_entries}
  defp decode_entries(
         <<locator::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>>,
         count,
         acc
       ) do
    decode_entries(rest, count - 1, [{key, locator} | acc])
  end

  defp decode_entries(<<>>, 0, acc), do: {:ok, Enum.reverse(acc)}
  defp decode_entries(_, _, _), do: {:error, :invalid_entries}

  @spec id(t()) :: id()
  def id(<<page_id::unsigned-big-32, _rest::binary>>), do: page_id
  def id(%{id: page_id}), do: page_id

  @spec key_locators(t()) :: [{binary(), Database.locator()}]
  def key_locators(
        <<_page_id::unsigned-big-32, key_count::unsigned-big-16, _right_key_offset::unsigned-big-32,
          _reserved::unsigned-big-48, entries_data::binary>>
      ) do
    {:ok, key_locators} = decode_entries(entries_data, key_count, [])
    key_locators
  end

  def key_locators(%{key_locators: kvs}), do: kvs

  @spec key_count(t()) :: non_neg_integer()
  def key_count(<<_page_id::unsigned-big-32, key_count::unsigned-big-16, _rest::binary>>), do: key_count

  def key_count(%{key_locators: kvs}), do: length(kvs)

  @spec keys(t()) :: [binary()]
  def keys(page) when is_binary(page) do
    page |> key_locators() |> Enum.map(fn {key, _locator} -> key end)
  end

  @spec left_key(t()) :: binary() | nil
  def left_key(<<_page_id::unsigned-big-32, 0::unsigned-big-16, _rest::binary>>), do: nil

  def left_key(
        <<_page_id::unsigned-big-32, _key_count::unsigned-big-16, _right_key_offset::unsigned-big-32,
          _reserved::unsigned-big-48, _locator::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len),
          _rest::binary>>
      ) do
    key
  end

  @spec right_key(t()) :: binary() | nil
  def right_key(<<_page_id::unsigned-big-32, 0::unsigned-big-16, _rest::binary>>), do: nil

  def right_key(<<_page_id::unsigned-big-32, _key_count::unsigned-big-16, 0::unsigned-big-32, _rest::binary>>), do: nil

  def right_key(page) when is_binary(page) do
    <<_page_id::unsigned-big-32, _key_count::unsigned-big-16, right_key_offset_from_end::unsigned-big-32,
      _reserved::unsigned-big-48, _payload::binary>> = page

    page_size = byte_size(page)
    key_start = page_size - right_key_offset_from_end
    <<_prefix::binary-size(key_start), key_len::unsigned-big-16, key::binary-size(key_len), _locator::binary>> = page
    key
  end

  @spec locator_for_key(t(), Bedrock.key()) :: {:ok, Database.locator()} | {:error, :not_found}
  def locator_for_key(
        <<_page_id::unsigned-big-32, key_count::unsigned-big-16, _right_key_offset::unsigned-big-32,
          _reserved::unsigned-big-48, entries_data::binary>>,
        target_key
      ),
      do: search_entries_for_key(entries_data, key_count, target_key)

  @spec search_entries_for_key(binary(), non_neg_integer(), Bedrock.key()) ::
          {:ok, Database.locator()} | {:error, :not_found}
  defp search_entries_for_key(_data, 0, _target_key), do: {:error, :not_found}

  defp search_entries_for_key(
         <<locator::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>>,
         count,
         target_key
       ) do
    if key == target_key do
      {:ok, locator}
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
         <<_locator::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>>,
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
          {:ok, {key :: binary(), locator :: binary()}} | :out_of_bounds
  def decode_entry_at_position(_entries_data, position, key_count) when position >= key_count, do: :out_of_bounds

  def decode_entry_at_position(entries_data, 0, _key_count) do
    <<locator::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len), _rest::binary>> = entries_data
    {:ok, {key, locator}}
  end

  def decode_entry_at_position(
        <<_locator::binary-size(8), key_len::unsigned-big-16, _key::binary-size(key_len), rest::binary>>,
        position,
        key_count
      ) do
    decode_entry_at_position(rest, position - 1, key_count)
  end

  def decode_entry_at_position(_, _, _), do: :out_of_bounds

  @spec stream_key_locators_in_range(Enumerable.t(t()), Bedrock.key(), Bedrock.key()) ::
          Enumerable.t({Bedrock.key(), Database.locator()})
  def stream_key_locators_in_range(pages_stream, start_key, end_key) do
    pages_stream
    |> Stream.flat_map(fn page -> key_locators(page) end)
    |> Stream.drop_while(fn {key, _locator} -> key < start_key end)
    |> Stream.take_while(fn {key, _locator} -> key < end_key end)
  end

  @doc """
  Checks if a page is empty (has no keys).
  """
  @spec empty?(t() | binary()) :: boolean()
  def empty?(page), do: key_count(page) == 0

  defp calculate_rightmost_key_offset(nil), do: 0
  defp calculate_rightmost_key_offset(key), do: 2 + byte_size(key)
end
