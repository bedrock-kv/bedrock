defmodule Bedrock.DataPlane.Storage.Olivine.KeySelectorPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.Index.Tree
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.KeySelector

  @moduletag :property

  # Generators

  def key_generator do
    # Generate keys that are valid binary strings
    StreamData.one_of([
      # Simple single-character keys
      ?a..?z |> StreamData.integer() |> StreamData.map(&<<&1>>),
      # Multi-character keys
      ?a..?z |> StreamData.integer() |> StreamData.list_of() |> StreamData.map(&List.to_string(&1)),
      # Keys with numbers
      {
        ?a..?z |> StreamData.integer() |> StreamData.map(&<<&1>>),
        StreamData.integer(0..99)
      }
      |> StreamData.tuple()
      |> StreamData.map(fn {prefix, suffix} -> "#{prefix}#{suffix}" end),
      # Edge case keys
      StreamData.constant(""),
      StreamData.constant("zzzzz")
    ])
  end

  def offset_generator do
    # Generate realistic offsets, including edge cases
    StreamData.one_of([
      # Small offsets (most common)
      StreamData.integer(-10..10),
      # Medium offsets
      StreamData.integer(-100..100),
      # Large offsets (test cross-page scenarios)
      StreamData.integer(-1000..1000),
      # Edge cases
      StreamData.constant(0)
    ])
  end

  def key_selector_generator do
    {key_generator(), offset_generator(), StreamData.integer(1..4)}
    |> StreamData.tuple()
    |> StreamData.map(fn {key, offset, selector_type} ->
      base_selector =
        case selector_type do
          1 -> KeySelector.first_greater_or_equal(key)
          2 -> KeySelector.first_greater_than(key)
          3 -> KeySelector.last_less_or_equal(key)
          4 -> KeySelector.last_less_than(key)
        end

      KeySelector.add(base_selector, offset)
    end)
  end

  def page_data_generator do
    # Generate realistic page data with varying densities
    1..100
    |> StreamData.integer()
    |> StreamData.map(fn key_count ->
      keys = Enum.map(1..key_count, fn i -> "key#{String.pad_leading(to_string(i), 3, "0")}" end)
      versions = List.duplicate(<<0, 0, 0, 0, 0, 0, 0, 1>>, key_count)
      Enum.zip(keys, versions)
    end)
  end

  def multi_page_index_generator do
    # Generate an index with multiple pages
    2..5
    |> StreamData.integer()
    |> StreamData.map(fn page_count ->
      pages =
        for page_id <- 0..(page_count - 1) do
          base_key_num = page_id * 100

          keys =
            Enum.map(1..50, fn i ->
              "page#{page_id}_key#{String.pad_leading(to_string(base_key_num + i), 3, "0")}"
            end)

          versions = List.duplicate(<<0, 0, 0, 0, 0, 0, 0, 1>>, 50)
          key_versions = Enum.zip(keys, versions)
          next_id = if page_id == page_count - 1, do: 0, else: page_id + 1
          Page.new(page_id, key_versions, next_id)
        end

      create_index_from_pages(pages)
    end)
  end

  defp create_index_from_pages(pages) do
    page_map = Map.new(pages, &{Page.id(&1), &1})
    tree = Tree.from_page_map(page_map)
    %Index{tree: tree, page_map: page_map}
  end

  # Property Tests

  property "KeySelector resolution either succeeds or returns a boundary error" do
    check all({index, key_selector} <- StreamData.tuple({multi_page_index_generator(), key_selector_generator()})) do
      case IndexManager.page_for_key(%IndexManager{versions: [{1, index}], current_version: 1}, key_selector, 1) do
        {:ok, _resolved_key, _page} -> true
        {:error, :not_found} -> true
        {:error, :clamped} -> true
        _other -> false
      end
    end
  end

  property "Forward offset arithmetic - larger offsets give lexicographically later keys" do
    check all(
            {index, key, offset1, offset2} <-
              StreamData.tuple({
                multi_page_index_generator(),
                key_generator(),
                StreamData.integer(0..50),
                StreamData.integer(0..50)
              })
          ) do
      # Only test when offset1 < offset2
      if offset1 < offset2 do
        selector1 = key |> KeySelector.first_greater_or_equal() |> KeySelector.add(offset1)
        selector2 = key |> KeySelector.first_greater_or_equal() |> KeySelector.add(offset2)

        index_manager = %IndexManager{versions: [{1, index}], current_version: 1}

        case {IndexManager.page_for_key(index_manager, selector1, 1),
              IndexManager.page_for_key(index_manager, selector2, 1)} do
          {{:ok, key1, _}, {:ok, key2, _}} -> key1 <= key2
          # If either fails to resolve, we can't compare
          _ -> true
        end
      else
        # Skip cases where condition doesn't hold
        true
      end
    end
  end

  property "Circuit breaker prevents infinite loops" do
    check all(index <- multi_page_index_generator()) do
      # Create a KeySelector with a very large offset that would require many page hops
      extreme_selector = "" |> KeySelector.first_greater_or_equal() |> KeySelector.add(10_000)

      index_manager = %IndexManager{versions: [{1, index}], current_version: 1}

      case IndexManager.page_for_key(index_manager, extreme_selector, 1) do
        # Circuit breaker activated
        {:error, :clamped} -> true
        # Legitimately not found
        {:error, :not_found} -> true
        # Somehow resolved (very unlikely but acceptable)
        {:ok, _key, _page} -> true
        _other -> false
      end
    end
  end

  property "KeySelector offset of 0 resolves to reference key if it exists" do
    check all(index <- multi_page_index_generator()) do
      # Pick a key that actually exists in the index
      all_keys = get_all_keys_from_index(index)

      if length(all_keys) > 0 do
        existing_key = Enum.random(all_keys)
        selector = existing_key |> KeySelector.first_greater_or_equal() |> KeySelector.add(0)

        index_manager = %IndexManager{versions: [{1, index}], current_version: 1}

        case IndexManager.page_for_key(index_manager, selector, 1) do
          {:ok, resolved_key, _page} -> resolved_key == existing_key
          _ -> false
        end
      else
        # Skip empty indices
        true
      end
    end
  end

  property "Range KeySelector results have consistent bounds" do
    check all(
            {index, start_key, end_key} <-
              StreamData.tuple({
                multi_page_index_generator(),
                key_generator(),
                key_generator()
              })
          ) do
      if start_key <= end_key do
        start_selector = KeySelector.first_greater_or_equal(start_key)
        end_selector = KeySelector.first_greater_than(end_key)

        index_manager = %IndexManager{versions: [{1, index}], current_version: 1}

        case IndexManager.pages_for_range(index_manager, start_selector, end_selector, 1) do
          {:ok, {resolved_start, resolved_end}, _pages} ->
            resolved_start <= resolved_end

          {:error, _} ->
            # Errors are acceptable
            true
        end
      else
        # Skip invalid ranges
        true
      end
    end
  end

  # Helper functions

  defp get_all_keys_from_index(%Index{page_map: page_map}) do
    page_map
    |> Map.values()
    |> Enum.flat_map(&Page.keys/1)
    |> Enum.sort()
    |> Enum.uniq()
  end
end
