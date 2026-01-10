defmodule Bedrock.DataPlane.CommitProxy.RoutingDataTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.RoutingData

  describe "new_empty/0" do
    test "creates empty routing data with all fields initialized" do
      routing_data = RoutingData.new_empty()

      assert %RoutingData{} = routing_data
      assert is_reference(routing_data.shard_table)
      assert routing_data.log_map == %{}
      assert routing_data.log_services == %{}
      assert routing_data.replication_factor == 1
    end

    test "creates empty ETS ordered_set table" do
      routing_data = RoutingData.new_empty()

      # Table should be empty
      assert :ets.tab2list(routing_data.shard_table) == []

      # Table should be ordered_set (verify by type info)
      info = :ets.info(routing_data.shard_table)
      assert info[:type] == :ordered_set

      RoutingData.cleanup(routing_data)
    end
  end

  describe "insert_log/2" do
    test "adds log to empty log_map at index 0" do
      routing_data = RoutingData.new_empty()

      updated = RoutingData.insert_log(routing_data, "log-1")

      assert updated.log_map == %{0 => "log-1"}

      RoutingData.cleanup(routing_data)
    end

    test "adds logs at sequential indices" do
      routing_data = RoutingData.new_empty()

      updated =
        routing_data
        |> RoutingData.insert_log("log-a")
        |> RoutingData.insert_log("log-b")
        |> RoutingData.insert_log("log-c")

      assert updated.log_map == %{0 => "log-a", 1 => "log-b", 2 => "log-c"}

      RoutingData.cleanup(routing_data)
    end

    test "does not modify other fields" do
      routing_data = RoutingData.new_empty()
      original_table = routing_data.shard_table

      updated = RoutingData.insert_log(routing_data, "log-1")

      assert updated.shard_table == original_table
      assert updated.log_services == %{}
      assert updated.replication_factor == 1

      RoutingData.cleanup(routing_data)
    end
  end

  describe "remove_log/2" do
    test "removes log from log_map" do
      routing_data =
        RoutingData.new_empty()
        |> RoutingData.insert_log("log-a")
        |> RoutingData.insert_log("log-b")
        |> RoutingData.insert_log("log-c")

      updated = RoutingData.remove_log(routing_data, "log-b")

      # Should reindex to maintain contiguous indices
      assert updated.log_map == %{0 => "log-a", 1 => "log-c"}

      RoutingData.cleanup(routing_data)
    end

    test "removes last log" do
      routing_data =
        RoutingData.new_empty()
        |> RoutingData.insert_log("log-a")
        |> RoutingData.insert_log("log-b")

      updated = RoutingData.remove_log(routing_data, "log-b")

      assert updated.log_map == %{0 => "log-a"}

      RoutingData.cleanup(routing_data)
    end

    test "removes first log and reindexes" do
      routing_data =
        RoutingData.new_empty()
        |> RoutingData.insert_log("log-a")
        |> RoutingData.insert_log("log-b")
        |> RoutingData.insert_log("log-c")

      updated = RoutingData.remove_log(routing_data, "log-a")

      assert updated.log_map == %{0 => "log-b", 1 => "log-c"}

      RoutingData.cleanup(routing_data)
    end

    test "no-op if log not found" do
      routing_data = RoutingData.insert_log(RoutingData.new_empty(), "log-a")

      updated = RoutingData.remove_log(routing_data, "nonexistent")

      assert updated.log_map == %{0 => "log-a"}

      RoutingData.cleanup(routing_data)
    end

    test "handles removing from empty log_map" do
      routing_data = RoutingData.new_empty()

      updated = RoutingData.remove_log(routing_data, "nonexistent")

      assert updated.log_map == %{}

      RoutingData.cleanup(routing_data)
    end
  end

  describe "put_log_service/3" do
    test "adds service ref to log_services" do
      routing_data = RoutingData.new_empty()

      updated = RoutingData.put_log_service(routing_data, "log-1", {:log_1, :node@host})

      assert updated.log_services == %{"log-1" => {:log_1, :node@host}}

      RoutingData.cleanup(routing_data)
    end

    test "adds multiple service refs" do
      routing_data = RoutingData.new_empty()

      updated =
        routing_data
        |> RoutingData.put_log_service("log-1", {:log_1, :n1@host})
        |> RoutingData.put_log_service("log-2", {:log_2, :n2@host})

      assert updated.log_services == %{
               "log-1" => {:log_1, :n1@host},
               "log-2" => {:log_2, :n2@host}
             }

      RoutingData.cleanup(routing_data)
    end

    test "overwrites existing service ref" do
      routing_data = RoutingData.put_log_service(RoutingData.new_empty(), "log-1", {:old_ref, :old_node})

      updated = RoutingData.put_log_service(routing_data, "log-1", {:new_ref, :new_node})

      assert updated.log_services == %{"log-1" => {:new_ref, :new_node}}

      RoutingData.cleanup(routing_data)
    end

    test "does not modify other fields" do
      routing_data = RoutingData.insert_log(RoutingData.new_empty(), "log-1")

      updated = RoutingData.put_log_service(routing_data, "log-1", {:log_1, :node@host})

      assert updated.log_map == %{0 => "log-1"}
      assert updated.replication_factor == 1

      RoutingData.cleanup(routing_data)
    end
  end

  describe "delete_log_service/2" do
    test "removes service ref from log_services" do
      routing_data =
        RoutingData.new_empty()
        |> RoutingData.put_log_service("log-1", {:log_1, :n1@host})
        |> RoutingData.put_log_service("log-2", {:log_2, :n2@host})

      updated = RoutingData.delete_log_service(routing_data, "log-1")

      assert updated.log_services == %{"log-2" => {:log_2, :n2@host}}

      RoutingData.cleanup(routing_data)
    end

    test "no-op if log not found" do
      routing_data = RoutingData.put_log_service(RoutingData.new_empty(), "log-1", {:log_1, :node@host})

      updated = RoutingData.delete_log_service(routing_data, "nonexistent")

      assert updated.log_services == %{"log-1" => {:log_1, :node@host}}

      RoutingData.cleanup(routing_data)
    end

    test "handles deleting from empty log_services" do
      routing_data = RoutingData.new_empty()

      updated = RoutingData.delete_log_service(routing_data, "nonexistent")

      assert updated.log_services == %{}

      RoutingData.cleanup(routing_data)
    end
  end

  describe "set_replication_factor/2" do
    test "updates replication factor" do
      routing_data = RoutingData.new_empty()

      updated = RoutingData.set_replication_factor(routing_data, 3)

      assert updated.replication_factor == 3

      RoutingData.cleanup(routing_data)
    end

    test "does not modify other fields" do
      routing_data =
        RoutingData.new_empty()
        |> RoutingData.insert_log("log-1")
        |> RoutingData.put_log_service("log-1", {:log_1, :node@host})

      updated = RoutingData.set_replication_factor(routing_data, 5)

      assert updated.log_map == %{0 => "log-1"}
      assert updated.log_services == %{"log-1" => {:log_1, :node@host}}

      RoutingData.cleanup(routing_data)
    end
  end

  describe "integration: typical usage pattern" do
    test "builds complete routing data incrementally" do
      # Start empty
      routing_data = RoutingData.new_empty()

      # Add logs (as they arrive via metadata)
      routing_data =
        routing_data
        |> RoutingData.insert_log("log-1")
        |> RoutingData.insert_log("log-2")
        |> RoutingData.insert_log("log-3")

      # Add service refs (how to reach each log)
      routing_data =
        routing_data
        |> RoutingData.put_log_service("log-1", {:log_1, :n1@host})
        |> RoutingData.put_log_service("log-2", {:log_2, :n2@host})
        |> RoutingData.put_log_service("log-3", {:log_3, :n3@host})

      # Add shard entries (via existing insert_shard/3)
      RoutingData.insert_shard(routing_data, "m", 0)
      RoutingData.insert_shard(routing_data, "z", 1)
      RoutingData.insert_shard(routing_data, <<0xFF, 0xFF>>, 2)

      # Set replication factor
      routing_data = RoutingData.set_replication_factor(routing_data, 3)

      # Verify complete state
      assert routing_data.log_map == %{0 => "log-1", 1 => "log-2", 2 => "log-3"}

      assert routing_data.log_services == %{
               "log-1" => {:log_1, :n1@host},
               "log-2" => {:log_2, :n2@host},
               "log-3" => {:log_3, :n3@host}
             }

      assert routing_data.replication_factor == 3

      assert :ets.tab2list(routing_data.shard_table) == [
               {"m", 0},
               {"z", 1},
               {<<0xFF, 0xFF>>, 2}
             ]

      RoutingData.cleanup(routing_data)
    end
  end
end
