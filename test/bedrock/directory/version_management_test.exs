defmodule Bedrock.Directory.VersionManagementTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.DirectoryHelpers
  import Mox

  alias Bedrock.Directory
  alias Bedrock.Directory.Layer
  alias Bedrock.Directory.Node

  setup do
    stub(MockRepo, :transaction, fn callback -> callback.(:mock_txn) end)
    :ok
  end

  setup :verify_on_exit!

  describe "version initialization" do
    test "initializes version on first write operation (create)" do
      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists([], nil)
      |> expect_directory_creation([])

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)
      assert {:ok, %Node{}} = Directory.create(layer, [])
    end

    test "does not reinitialize version if already set" do
      root_data = Bedrock.Key.pack({<<0, 1>>, ""})

      MockRepo
      |> expect_version_check_only()
      |> expect_version_check_only()
      |> expect_directory_exists(["test"], nil)
      |> expect_directory_exists([], root_data)
      |> expect_directory_creation(["test"])

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)
      assert {:ok, %Node{}} = Directory.create(layer, ["test"])
    end
  end

  describe "version compatibility checks" do
    test "allows reads from compatible versions" do
      test_data = Bedrock.Key.pack({<<0, 42>>, ""})

      MockRepo
      |> expect_directory_exists(["test"], test_data)
      |> expect_version_check_only()

      layer = Layer.new(MockRepo)
      assert {:ok, %Node{prefix: <<0, 42>>}} = Directory.open(layer, ["test"])
    end

    test "blocks reads from incompatible major version" do
      incompatible_version = <<2::little-32, 0::little-32, 0::little-32>>
      test_data = Bedrock.Key.pack({<<0, 42>>, ""})

      MockRepo
      |> expect_directory_exists(["test"], test_data)
      |> expect_version_check_only(incompatible_version)

      layer = Layer.new(MockRepo)
      assert {:error, :incompatible_directory_version} = Directory.open(layer, ["test"])
    end

    test "blocks writes to newer minor version" do
      newer_minor_version = <<1::little-32, 1::little-32, 0::little-32>>

      expect_version_check_only(MockRepo, newer_minor_version)
      # No more expectations - check_version will fail early

      layer = Layer.new(MockRepo)
      assert {:error, :directory_version_write_restricted} = Directory.create(layer, ["test"])
    end

    test "allows writes to same or older minor version" do
      root_data = Bedrock.Key.pack({<<0, 1>>, ""})

      MockRepo
      |> expect_version_check_only()
      |> expect_version_check_only()
      |> expect_directory_exists(["test"], nil)
      |> expect_directory_exists([], root_data)
      |> expect_directory_creation(["test"])

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> <<0, 42>> end)
      assert {:ok, %Node{}} = Directory.create(layer, ["test"])
    end
  end

  describe "version checks in operations" do
    test "remove operation checks version" do
      test_data = Bedrock.Key.pack({<<0, 42>>, ""})

      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists(["test"], test_data)
      |> expect_range_clear(["test"])

      layer = Layer.new(MockRepo)
      assert :ok = Directory.remove(layer, ["test"])
    end

    test "move operation checks version" do
      source_data = Bedrock.Key.pack({<<0, 42>>, ""})
      root_data = Bedrock.Key.pack({<<0, 1>>, ""})

      range_results = [
        {build_directory_key(["old"]), source_data}
      ]

      MockRepo
      |> expect_version_check_only()
      # ensure_version_initialized also checks
      |> expect_version_check_only()
      |> expect_directory_exists(["old"], source_data)
      |> expect_directory_exists(["new"], nil)
      |> expect_directory_exists([], root_data)
      |> expect_directory_exists(["old"], source_data)
      |> expect_range_scan(["old"], range_results)
      |> expect_directory_creation(["new"])
      |> expect_range_clear(["old"])

      layer = Layer.new(MockRepo)
      assert :ok = Directory.move(layer, ["old"], ["new"])
    end

    test "list operation checks version for reads" do
      MockRepo
      |> expect_version_check_only()
      |> expect(:range, fn :mock_txn, _range -> [] end)

      layer = Layer.new(MockRepo)
      assert {:ok, []} = Directory.list(layer, [])
    end

    test "exists? operation does not check version directly" do
      test_data = Bedrock.Key.pack({<<0, 42>>, ""})

      expect_directory_exists(MockRepo, ["test"], test_data)
      layer = Layer.new(MockRepo)
      assert Directory.exists?(layer, ["test"])
    end
  end
end
