defmodule Bedrock.Cluster.Gateway.TransactionBuilder.FetchingPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Fetching

  describe "fetch_from_stack properties" do
    property "empty stack always returns :error" do
      check all(key <- binary()) do
        assert :error = Fetching.fetch_from_stack(key, [])
      end
    end

    property "writes take precedence over reads in same frame" do
      check all(
              key <- binary(),
              write_value <- term(),
              read_value <- term(),
              other_writes <- map_of(binary(), term()),
              other_reads <- map_of(binary(), term())
            ) do
        # Ensure the key isn't in other maps to avoid conflicts
        other_writes = Map.delete(other_writes, key)
        other_reads = Map.delete(other_reads, key)

        # Create frame with both read and write for the same key
        reads = Map.put(other_reads, key, read_value)
        writes = Map.put(other_writes, key, write_value)
        stack = [{reads, writes}]

        assert {:ok, ^write_value} = Fetching.fetch_from_stack(key, stack)
      end
    end

    property "first stack frame takes precedence over later frames" do
      check all(
              key <- binary(),
              first_value <- term(),
              later_value <- term(),
              stack_depth <- integer(2..5)
            ) do
        # Create first frame with the value
        first_frame = {%{key => first_value}, %{}}

        # Create later frames that might have the same key
        later_frames =
          for _i <- 1..stack_depth do
            {%{key => later_value}, %{key => later_value}}
          end

        stack = [first_frame | later_frames]

        assert {:ok, ^first_value} = Fetching.fetch_from_stack(key, stack)
      end
    end

    property "searches through frames until finding key" do
      check all(
              key <- binary(),
              target_value <- term(),
              empty_frame_count <- integer(0..3),
              frames_after <- integer(0..3)
            ) do
        # Create empty frames before the target
        empty_frames =
          for _i <- 1..empty_frame_count//1 do
            {%{}, %{}}
          end

        # Create the frame with our target
        target_frame = {%{key => target_value}, %{}}

        # Create frames after (should be ignored)
        later_frames =
          for _i <- 1..frames_after//1 do
            {%{key => "should_not_find"}, %{}}
          end

        stack = empty_frames ++ [target_frame] ++ later_frames

        assert {:ok, ^target_value} = Fetching.fetch_from_stack(key, stack)
      end
    end

    property "returns :error when key not found in any frame" do
      check all(
              key <- binary(),
              stack_frames <- list_of(stack_frame_without_key_generator(key), max_length: 5)
            ) do
        assert :error = Fetching.fetch_from_stack(key, stack_frames)
      end
    end
  end

  describe "fastest_storage_server_for_key properties" do
    property "returns nil when no servers match key range" do
      check all(
              key <- binary(),
              server_map <- map_of(key_range_generator(), pid_generator())
            ) do
        # Filter out any ranges that would actually contain our key
        filtered_map =
          server_map
          |> Enum.reject(fn {{min_key, max_key}, _pid} ->
            key_in_range?(key, min_key, max_key)
          end)
          |> Map.new()

        if map_size(filtered_map) > 0 do
          assert nil == Fetching.fastest_storage_server_for_key(filtered_map, key)
        end
      end
    end

    property "returns server when key is in range" do
      check all(
              key <- binary(),
              server_pid <- pid_generator()
            ) do
        # Create a range that definitely contains our key
        # Key to end always contains the key
        range = {key, :end}
        server_map = %{range => server_pid}

        assert ^server_pid = Fetching.fastest_storage_server_for_key(server_map, key)
      end
    end

    property "handles :end boundary correctly" do
      check all(
              key <- binary(),
              server_pid <- pid_generator()
            ) do
        # Range with :end should include everything >= min_key
        # Empty string to end includes everything
        range = {"", :end}
        server_map = %{range => server_pid}

        assert ^server_pid = Fetching.fastest_storage_server_for_key(server_map, key)
      end
    end
  end

  describe "storage_servers_for_key properties" do
    property "returns empty list when no teams cover key range" do
      check all(key <- binary()) do
        layout = %{
          storage_teams: [],
          services: %{}
        }

        assert [] = Fetching.storage_servers_for_key(layout, key)
      end
    end

    property "filters out down storage servers" do
      check all(key <- binary()) do
        layout = %{
          storage_teams: [
            %{
              key_range: {"", :end},
              storage_ids: ["up_server", "down_server"]
            }
          ],
          services: %{
            "up_server" => %{kind: :storage, status: {:up, :up_pid}},
            "down_server" => %{kind: :storage, status: {:down, nil}}
          }
        }

        result = Fetching.storage_servers_for_key(layout, key)

        # Should only include the up server
        assert [{{"", :end}, :up_pid}] = result
      end
    end
  end

  # Custom generators
  defp stack_frame_without_key_generator(excluded_key) do
    gen all(
          reads <- map_of(binary_excluding(excluded_key), term()),
          writes <- map_of(binary_excluding(excluded_key), term())
        ) do
      {reads, writes}
    end
  end

  defp binary_excluding(excluded_key) do
    StreamData.filter(binary(), fn key -> key != excluded_key end)
  end

  defp key_range_generator do
    gen all(
          min_key <- binary(),
          max_key <- one_of([binary(), constant(:end)])
        ) do
      {min_key, max_key}
    end
  end

  defp pid_generator do
    constant(self())
  end

  defp key_in_range?(key, min_key, :end) do
    key >= min_key
  end

  defp key_in_range?(key, min_key, max_key) when is_binary(max_key) do
    key >= min_key and key < max_key
  end
end
