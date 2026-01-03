defmodule Bedrock.Directory.VersionManagementTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.DirectoryHelpers
  import Mox

  alias Bedrock.Directory
  alias Bedrock.Keyspace

  setup do
    stub(MockRepo, :transact, fn callback -> callback.() end)
    :ok
  end

  setup :verify_on_exit!

  describe "version initialization" do
    test "initializes version on first write operation (create)" do
      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists(["test"], nil)
      |> expect_directory_creation(["test"])

      layer = Directory.root(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)
      # Test version initialization via subdirectory creation
      assert {:ok, %Directory.Node{}} = Directory.create(layer, ["test"])
    end

    test "does not reinitialize version if already set" do
      MockRepo
      |> expect_version_check()
      |> expect_version_check()
      |> expect_directory_exists(["test"], nil)
      |> expect_directory_creation(["test"])

      layer = Directory.root(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)
      assert {:ok, %Directory.Node{}} = Directory.create(layer, ["test"])
    end
  end

  describe "version compatibility checks" do
    test "allows reads from compatible versions" do
      test_data = {<<0, 42>>, ""}

      MockRepo
      |> expect_directory_exists(["test"], test_data)
      |> expect_version_check()

      layer = Directory.root(MockRepo)
      assert {:ok, %Directory.Node{prefix: <<0, 42>>}} = Directory.open(layer, ["test"])
    end

    test "blocks reads from incompatible major version" do
      incompatible_version = <<2::little-32, 0::little-32, 0::little-32>>
      test_data = {<<0, 42>>, ""}

      MockRepo
      |> expect_directory_exists(["test"], test_data)
      |> expect_version_check(incompatible_version)

      layer = Directory.root(MockRepo)
      assert {:error, :incompatible_directory_version} = Directory.open(layer, ["test"])
    end

    test "blocks writes to newer minor version" do
      newer_minor_version = <<1::little-32, 1::little-32, 0::little-32>>

      expect_version_check(MockRepo, newer_minor_version)
      # No more expectations - check_version will fail early

      layer = Directory.root(MockRepo)
      assert {:error, :directory_version_write_restricted} = Directory.create(layer, ["test"])
    end

    test "allows writes to same or older minor version" do
      MockRepo
      |> expect_version_check()
      |> expect_version_check()
      |> expect_directory_exists(["test"], nil)
      |> expect_directory_creation(["test"])

      layer = Directory.root(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)
      assert {:ok, %Directory.Node{}} = Directory.create(layer, ["test"])
    end
  end

  describe "version checks in operations" do
    test "remove operation checks version" do
      test_data = {<<0, 42>>, ""}

      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists(["test"], test_data)
      |> expect_range_clear(["test"])

      layer = Directory.root(MockRepo)
      assert :ok = Directory.remove(layer, ["test"])
    end

    test "move operation checks version" do
      source_data = {<<0, 42>>, ""}

      MockRepo
      |> expect_version_initialization()
      # Source exists check
      |> expect_directory_exists(["old"], source_data)
      # Destination doesn't exist check
      |> expect_directory_exists(["new"], nil)
      # Source fetch for move operation (gets called again)
      |> expect_directory_exists(["old"], source_data)
      # Range scan to get source + all children - now using keyspace get_range/2
      |> expect(:get_range, fn start_key, end_key ->
        assert is_binary(start_key)
        assert is_binary(end_key)
        # Return packed data since this is a raw key scan
        old_key = build_directory_key(["old"])
        packed_source_data = Bedrock.Encoding.Tuple.pack(source_data)
        [{old_key, packed_source_data}]
      end)
      # Put destination directory - keyspace-aware
      |> expect(:put, fn %Keyspace{}, ["new"], value ->
        assert {<<0, 42>>, ""} == value
        :ok
      end)
      # Clear source range - now using clear_range/3 with binary keys
      |> expect(:clear_range, fn start_key, end_key ->
        assert is_binary(start_key)
        assert is_binary(end_key)
        :ok
      end)

      layer = Directory.root(MockRepo)
      assert :ok = Directory.move(layer, ["old"], ["new"])
    end

    test "list operation checks version for reads" do
      MockRepo
      |> expect_version_check()
      |> expect(:get_range, fn prefix_key ->
        assert is_binary(prefix_key)
        []
      end)

      layer = Directory.root(MockRepo)
      assert {:ok, []} = Directory.list(layer, [])
    end

    test "exists? operation does not check version directly" do
      test_data = {<<0, 42>>, ""}

      expect_directory_exists(MockRepo, ["test"], test_data)
      layer = Directory.root(MockRepo)
      assert Directory.exists?(layer, ["test"])
    end
  end
end
