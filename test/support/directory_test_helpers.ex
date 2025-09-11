defmodule Bedrock.Test.DirectoryHelpers do
  @moduledoc """
  Shared helper functions for directory layer tests.
  Provides common mock expectations and utilities.
  """

  import ExUnit.Assertions
  import Mox

  alias Bedrock.Key
  alias Bedrock.KeyRange

  # Version constants
  @version_key <<254, 6, 1, 118, 101, 114, 115, 105, 111, 110, 0, 0>>
  @current_version <<1::little-32, 0::little-32, 0::little-32>>

  @doc """
  Expects version initialization sequence (check, ensure, init).
  Used when creating the first directory in a transaction.
  """
  def expect_version_initialization(repo, storage \\ nil) do
    repo
    |> expect(:get, fn :mock_txn, @version_key -> nil end)
    |> expect(:get, fn :mock_txn, @version_key -> nil end)
    |> expect(:put, fn :mock_txn, @version_key, @current_version ->
      if storage, do: Agent.update(storage, &Map.put(&1, @version_key, @current_version))
      :ok
    end)
  end

  @doc """
  Expects a single version check for read operations.
  """
  def expect_version_check_only(repo, version \\ @current_version) do
    expect(repo, :get, fn :mock_txn, @version_key -> version end)
  end

  @doc """
  Expects a directory existence check.
  """
  def expect_directory_exists(repo, path, result) do
    expected_key = build_directory_key(path)

    expect(repo, :get, fn :mock_txn, ^expected_key -> result end)
  end

  @doc """
  Expects directory creation with the given packed value.
  """
  def expect_directory_creation(repo, path, packed_value) do
    expected_key = build_directory_key(path)

    expect(repo, :put, fn :mock_txn, ^expected_key, value ->
      assert ^packed_value = Key.unpack(value)
      :ok
    end)
  end

  @doc """
  Expects directory creation without value assertion.
  """
  def expect_directory_creation(repo, path) do
    expected_key = build_directory_key(path)

    expect(repo, :put, fn :mock_txn, ^expected_key, _value -> :ok end)
  end

  @doc """
  Expects parent directory existence check.
  """
  def expect_parent_exists(repo, path, result \\ nil) do
    if path == [] do
      # Root has no parent
      repo
    else
      parent_path = Enum.drop(path, -1)
      parent_key = build_directory_key(parent_path)
      result = result || Key.pack({<<0, 1>>, ""})

      expect(repo, :get, fn :mock_txn, ^parent_key -> result end)
    end
  end

  @doc """
  Expects a range scan over a directory's children.
  """
  def expect_range_scan(repo, path, results) do
    expected_range = KeyRange.from_prefix(build_directory_key(path))

    expect(repo, :range, fn :mock_txn, ^expected_range -> results end)
  end

  @doc """
  Expects a range scan with options.
  """
  def expect_range_scan(repo, path, results, opts) do
    expected_range = KeyRange.from_prefix(build_directory_key(path))

    expect(repo, :range, fn :mock_txn, ^expected_range, actual_opts ->
      # Assert on any specific options we care about
      if opts[:limit], do: assert(actual_opts[:limit] == opts[:limit])
      results
    end)
  end

  @doc """
  Expects clearing a range (used in remove/move operations).
  """
  def expect_range_clear(repo, path) do
    expected_range = KeyRange.from_prefix(build_directory_key(path))

    expect(repo, :clear_range, fn :mock_txn, ^expected_range -> :ok end)
  end

  @doc """
  Expects prefix collision check during directory creation.
  Note: This only handles the range check and full prefix check.
  For ancestor checks, use a custom helper in your test.
  """
  def expect_prefix_collision_check(repo, prefix, results \\ []) do
    expected_range = KeyRange.from_prefix(prefix)

    repo
    |> expect(:range, fn :mock_txn, ^expected_range, opts ->
      assert opts[:limit] == 1
      results
    end)
    |> expect(:get, fn :mock_txn, ^prefix -> nil end)
  end

  @doc """
  Builds the database key for a directory path.
  """
  def build_directory_key([]), do: <<254>>
  def build_directory_key(path), do: <<254>> <> Key.pack(path)

  @doc """
  Helper to pack directory node data.
  """
  def pack_directory_value(prefix, layer \\ "", version \\ nil, metadata \\ nil) do
    case {version, metadata} do
      {nil, nil} -> Key.pack({prefix, layer})
      {version, nil} -> Key.pack({prefix, layer, version})
      {nil, metadata} -> Key.pack({prefix, layer, nil, metadata})
      {version, metadata} -> Key.pack({prefix, layer, version, metadata})
    end
  end
end
