defmodule Bedrock.Directory.VersionManagementTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.DirectoryHelpers
  import Mox

  alias Bedrock.Directory

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
      test_data = Bedrock.Key.pack({<<0, 42>>, ""})

      MockRepo
      |> expect_directory_exists(["test"], test_data)
      |> expect_version_check()

      layer = Directory.root(MockRepo)
      assert {:ok, %Directory.Node{prefix: <<0, 42>>}} = Directory.open(layer, ["test"])
    end

    test "blocks reads from incompatible major version" do
      incompatible_version = <<2::little-32, 0::little-32, 0::little-32>>
      test_data = Bedrock.Key.pack({<<0, 42>>, ""})

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
      test_data = Bedrock.Key.pack({<<0, 42>>, ""})

      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists(["test"], test_data)
      |> expect_range_clear(["test"])

      layer = Directory.root(MockRepo)
      assert :ok = Directory.remove(layer, ["test"])
    end

    test "move operation checks version" do
      source_data = Bedrock.Key.pack({<<0, 42>>, ""})
      version_key = <<254, 6, 1, 118, 101, 114, 115, 105, 111, 110, 0, 0>>
      current_version = <<1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>
      old_key = <<254, 6, 1, 111, 108, 100, 0, 0>>
      new_key = <<254, 6, 1, 110, 101, 119, 0, 0>>

      MockRepo
      # Version checks (2 calls)
      |> expect(:get, fn ^version_key -> current_version end)
      |> expect(:get, fn ^version_key -> current_version end)
      # Source exists check
      |> expect(:get, fn ^old_key -> source_data end)
      # Destination doesn't exist check
      |> expect(:get, fn ^new_key -> nil end)
      # Source fetch for move operation (gets called again)
      |> expect(:get, fn ^old_key -> source_data end)
      # Range scan to get source + all children
      |> expect(:get_range, fn {^old_key, <<254, 6, 1, 111, 108, 100, 0, 1>>} ->
        [{old_key, source_data}]
      end)
      # Put destination directory
      |> expect(:put, fn ^new_key, value ->
        assert {<<0, 42>>, ""} == Bedrock.Key.unpack(value)
        :ok
      end)
      # Clear source range
      |> expect(:clear_range, fn {^old_key, <<254, 6, 1, 111, 108, 100, 0, 1>>} ->
        :ok
      end)

      layer = Directory.root(MockRepo)
      assert :ok = Directory.move(layer, ["old"], ["new"])
    end

    test "list operation checks version for reads" do
      MockRepo
      |> expect_version_check()
      |> expect(:get_range, fn _range -> [] end)

      layer = Directory.root(MockRepo)
      assert {:ok, []} = Directory.list(layer, [])
    end

    test "exists? operation does not check version directly" do
      test_data = Bedrock.Key.pack({<<0, 42>>, ""})

      expect_directory_exists(MockRepo, ["test"], test_data)
      layer = Directory.root(MockRepo)
      assert Directory.exists?(layer, ["test"])
    end
  end
end
