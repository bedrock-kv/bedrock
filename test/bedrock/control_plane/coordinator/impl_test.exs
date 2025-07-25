defmodule Bedrock.ControlPlane.Coordinator.ImplTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.Impl

  # Helper function to create a fake storage server with custom response functions
  defp fake_storage_server(response_fn) do
    spawn(fn ->
      fake_storage_loop(response_fn)
    end)
  end

  defp fake_storage_loop(response_fn) do
    receive do
      {:"$gen_call", from, call} ->
        response = response_fn.(call)
        GenServer.reply(from, response)
        fake_storage_loop(response_fn)

      _ ->
        fake_storage_loop(response_fn)
    after
      5000 -> :ok
    end
  end

  describe "read_latest_config_from_raft_log/1" do
    test "returns error when raft log is empty" do
      # Create a fake empty raft log
      fake_raft_log = :empty_log

      result = Impl.read_latest_config_from_raft_log(fake_raft_log)

      # Should return error when log is empty
      assert {:error, :empty_log} = result
    end

    test "returns error when no config transactions found" do
      # Create a fake raft log with non-config transactions
      fake_raft_log = :non_config_log

      result = Impl.read_latest_config_from_raft_log(fake_raft_log)

      # Should return error when no config found
      assert {:error, _reason} = result
    end
  end

  describe "find_highest_version_storage_and_read_config/2" do
    test "returns default config when no storage workers available" do
      coordinator_nodes = [:node1, :node2, :node3]
      result = Impl.find_highest_version_storage_and_read_config([], coordinator_nodes)

      assert {0, _config} = result
    end
  end

  describe "try_read_config_in_sequence/2" do
    test "returns default config when storage list is empty" do
      coordinator_nodes = [:node1, :node2, :node3]
      result = Impl.try_read_config_in_sequence([], coordinator_nodes)

      assert {0, _config} = result
    end

    test "falls back to default config when storage workers fail" do
      coordinator_nodes = [:node1, :node2, :node3]
      storage_versions = [{:fake_storage, 100}, {:another_fake, 50}]

      result = Impl.try_read_config_in_sequence(storage_versions, coordinator_nodes)

      # Should fall back to default config since fake storage workers don't exist
      assert {0, _config} = result
    end
  end

  describe "get_storage_durable_version/1" do
    test "returns error for non-existent storage worker" do
      result = Impl.get_storage_durable_version(:fake_storage)

      # Should return error since storage worker doesn't exist
      assert {:error, _reason} = result
    end

    test "successfully gets durable version from fake storage server" do
      fake_storage =
        fake_storage_server(fn
          {:info, [:durable_version]} -> {:ok, %{durable_version: 42}}
          _ -> {:error, :not_implemented}
        end)

      result = Impl.get_storage_durable_version(fake_storage)

      assert {:ok, {^fake_storage, 42}} = result
    end
  end

  describe "version comparison and fallback logic" do
    test "selects storage worker with highest version and falls back on failure" do
      # Create fake storage servers with different behaviors
      config = %{test: "config_from_storage", nodes: [:node1, :node2, :node3]}

      storage_high =
        fake_storage_server(fn
          {:info, [:durable_version]} -> {:ok, %{durable_version: 100}}
          # No fetch handler - will fail config fetch
          _ -> {:error, :not_implemented}
        end)

      storage_broken =
        fake_storage_server(fn
          {:info, [:durable_version]} -> {:ok, %{durable_version: 75}}
          {:fetch, "\xff/system/config", 75, _opts} -> {:error, :not_found}
          _ -> {:error, :not_implemented}
        end)

      storage_low =
        fake_storage_server(fn
          {:info, [:durable_version]} ->
            {:ok, %{durable_version: 50}}

          {:fetch, "\xff/system/config", 50, _opts} ->
            bert_data = :erlang.term_to_binary({50, config})
            {:ok, bert_data}

          _ ->
            {:error, :not_implemented}
        end)

      coordinator_nodes = [:node1, :node2, :node3]
      storage_workers = [storage_high, storage_low, storage_broken]

      result =
        Impl.find_highest_version_storage_and_read_config(storage_workers, coordinator_nodes)

      # Should try storage_high (100) first, then storage_broken (75), then storage_low (50)
      # storage_high and storage_broken will fail config fetch, so should get config from storage_low
      assert {50, %{test: "config_from_storage", nodes: [:node1, :node2, :node3]}} = result
    end
  end
end
