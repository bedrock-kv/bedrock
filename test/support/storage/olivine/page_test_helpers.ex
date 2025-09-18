defmodule Bedrock.Test.Storage.Olivine.PageTestHelpers do
  @moduledoc """
  Test helper functions for Page operations that are only used in tests.
  """

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page

  @doc """
  Persists a single page to the database.
  """
  @spec persist_page_to_database(any(), Database.t(), Page.t()) :: :ok | {:error, term()}
  def persist_page_to_database(_index_manager, database, page) when is_map(page) do
    binary = from_map(page)
    next_id = page.next_id
    Database.store_page(database, Page.id(page), {binary, next_id})
  end

  def persist_page_to_database(_index_manager, database, page) when is_binary(page) do
    # For binary pages created with Page.new(), we can't access next_id directly
    # Since we're in a test context, assume next_id = 0 if not specified
    binary = from_map(page)
    # Default next_id for most test pages
    next_id = 0
    Database.store_page(database, Page.id(page), {binary, next_id})
  end

  @doc """
  Persists multiple pages to the database in batch.
  """
  @spec persist_pages_batch([Page.t()], Database.t()) :: :ok | {:error, term()}
  def persist_pages_batch(pages, database) do
    Enum.reduce_while(pages, :ok, fn page, :ok ->
      binary = from_map(page)
      next_id = if is_map(page), do: page.next_id, else: 0

      case Database.store_page(database, Page.id(page), {binary, next_id}) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  @doc """
  Determines if two ranges overlap using the exact algorithm from the specification.
  Returns true if ranges [AStart, AEnd] and [BStart, BEnd] overlap.
  """
  @spec ranges_overlap(Bedrock.key(), Bedrock.key(), Bedrock.key(), Bedrock.key()) :: boolean()
  def ranges_overlap(a_start, a_end, b_start, b_end) do
    not (a_end < b_start or b_end < a_start)
  end

  @doc """
  Checks if the page's keys are in sorted order.
  """
  @spec keys_are_sorted(Page.t() | binary()) :: boolean()
  def keys_are_sorted(page) do
    page_keys = Page.keys(page)
    page_keys == Enum.sort(page_keys)
  end

  @doc """
  Adds a key-version pair to a page using the real Page.apply_operations.
  """
  @spec add_key_to_page(Page.t(), binary(), Bedrock.version()) :: Page.t()
  def add_key_to_page(page, key, version) when is_binary(version) and byte_size(version) == 8 do
    Page.apply_operations(page, %{key => {:set, version}})
  end

  @doc """
  Walks the page chain starting from page 0 and returns page IDs in chain order.

  This function is useful for testing chain integrity and verifying that page
  operations maintain proper chain structure.

  ## Examples

      iex> page_map = %{0 => %{next_id: 2}, 2 => %{next_id: 5}, 5 => %{next_id: 0}}
      iex> PageTestHelpers.walk_page_chain(page_map)
      [0, 2, 5]

      iex> PageTestHelpers.walk_page_chain(%{})
      []
  """
  @spec walk_page_chain(map()) :: [non_neg_integer()]
  def walk_page_chain(page_map) when is_map_key(page_map, 0), do: walk_page_chain_from(page_map, 0, [])
  def walk_page_chain(_), do: []

  defp walk_page_chain_from(page_map, id, acc) do
    page = Map.fetch!(page_map, id)

    case page do
      %{next_id: 0} ->
        Enum.reverse([id | acc])

      %{next_id: next_id} ->
        walk_page_chain_from(page_map, next_id, [id | acc])
    end
  end

  @doc """
  Converts a page map to binary format for testing serialization.
  Only used in tests for page persistence and deserialization testing.
  """
  @spec from_map(Page.t()) :: binary()
  def from_map(%{key_versions: key_versions, id: id, next_id: _next_id}), do: encode_page_direct(id, key_versions)

  def from_map(binary) when is_binary(binary), do: binary

  @doc """
  Converts binary page data back to map format for testing.
  Only used in tests for page persistence and deserialization testing.
  """
  @spec to_map(binary()) :: {:ok, Page.t()} | {:error, :invalid_page}
  def to_map(
        <<id::unsigned-big-32, key_count::unsigned-big-16, _right_key_offset::unsigned-big-32,
          _reserved::unsigned-big-48, entries_data::binary>>
      ) do
    entries_data
    |> decode_entries(key_count, [])
    |> case do
      {:ok, key_versions} ->
        {:ok,
         %{
           id: id,
           next_id: 0,
           key_versions: key_versions
         }}

      error ->
        error
    end
  end

  def to_map(_), do: {:error, :invalid_page}

  # Direct page encoding using the efficient binary approach
  @spec encode_page_direct(Page.id(), [{binary(), Bedrock.version()}]) :: binary()
  defp encode_page_direct(id, key_versions) do
    key_count = length(key_versions)

    # Encode all entries as interleaved version-key pairs
    {entries_binary, last_key_offset} = encode_entries(key_versions, <<>>, 0)

    # Calculate right key offset: header (16 bytes) + entries up to last key
    right_key_offset = 16 + last_key_offset

    # Build the page with 16-byte header + interleaved entries
    <<id::unsigned-big-32, key_count::unsigned-big-16, right_key_offset::unsigned-big-32, 0::unsigned-big-48,
      entries_binary::binary>>
  end

  # Encode entries as interleaved version-key pairs and track last key offset
  @spec encode_entries([{binary(), Bedrock.version()}], binary(), non_neg_integer()) ::
          {binary(), non_neg_integer()}
  defp encode_entries([], acc, last_offset), do: {acc, last_offset}

  defp encode_entries([{key, version} | rest], acc, _last_offset) do
    key_len = byte_size(key)
    # version + key_len, then key starts
    new_last_offset = byte_size(acc) + 8 + 2
    entry = <<version::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len)>>
    encode_entries(rest, acc <> entry, new_last_offset)
  end

  # Decode interleaved version-key entries
  @spec decode_entries(binary(), non_neg_integer(), [{binary(), Bedrock.version()}]) ::
          {:ok, [{binary(), Bedrock.version()}]} | {:error, :invalid_entries}
  defp decode_entries(<<>>, 0, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_entries(
         <<version::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>>,
         count,
         acc
       )
       when count > 0 do
    decode_entries(rest, count - 1, [{key, version} | acc])
  end

  defp decode_entries(_, count, _) when count > 0, do: {:error, :invalid_entries}
  defp decode_entries(_, 0, _), do: {:error, :invalid_entries}
end
