defmodule Bedrock.Cluster.Gateway.TransactionBuilder.FetchingTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Fetching
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State

  defmodule TestKeyCodec do
    def encode_key(key) when is_binary(key), do: {:ok, key}
    def encode_key(_), do: :key_error
  end

  defmodule TestValueCodec do
    def encode_value(value), do: {:ok, value}
    def decode_value(value), do: {:ok, value}
  end

  def create_test_state(opts \\ []) do
    %State{
      state: :valid,
      gateway: Keyword.get(opts, :gateway, :test_gateway),
      transaction_system_layout:
        Keyword.get(opts, :transaction_system_layout, create_test_layout()),
      key_codec: Keyword.get(opts, :key_codec, TestKeyCodec),
      value_codec: Keyword.get(opts, :value_codec, TestValueCodec),
      read_version: Keyword.get(opts, :read_version),
      read_version_lease_expiration: Keyword.get(opts, :read_version_lease_expiration),
      reads: Keyword.get(opts, :reads, %{}),
      writes: Keyword.get(opts, :writes, %{}),
      stack: Keyword.get(opts, :stack, []),
      fastest_storage_servers: Keyword.get(opts, :fastest_storage_servers, %{}),
      fetch_timeout_in_ms: Keyword.get(opts, :fetch_timeout_in_ms, 100)
    }
  end

  def create_test_layout do
    %{
      sequencer: :test_sequencer,
      storage_teams: [
        %{
          key_range: {"", :end},
          storage_ids: ["storage1", "storage2"]
        }
      ],
      services: %{
        "storage1" => %{kind: :storage, status: {:up, :storage1_pid}},
        "storage2" => %{kind: :storage, status: {:up, :storage2_pid}}
      }
    }
  end

  describe "do_fetch/3" do
    test "returns value from writes cache" do
      state = create_test_state(writes: %{"cached_key" => "cached_value"})

      {new_state, result} = Fetching.do_fetch(state, "cached_key")

      assert result == {:ok, "cached_value"}
      # State unchanged when reading from writes
      assert new_state == state
    end

    test "returns value from reads cache" do
      state = create_test_state(reads: %{"read_key" => "read_value"})

      {new_state, result} = Fetching.do_fetch(state, "read_key")

      assert result == {:ok, "read_value"}
      # State unchanged when reading from reads
      assert new_state == state
    end

    test "writes cache takes precedence over reads cache" do
      state =
        create_test_state(
          reads: %{"key" => "old_value"},
          writes: %{"key" => "new_value"}
        )

      {new_state, result} = Fetching.do_fetch(state, "key")

      assert result == {:ok, "new_value"}
      assert new_state == state
    end

    test "fetches from storage when not in cache" do
      state = create_test_state(read_version: 12_345)

      storage_fetch_fn = fn :storage1_pid, "storage_key", 12_345, [timeout: 100] ->
        {:ok, "storage_value"}
      end

      opts = [storage_fetch_fn: storage_fetch_fn]

      {new_state, result} = Fetching.do_fetch(state, "storage_key", opts)

      assert result == {:ok, "storage_value"}
      assert new_state.reads == %{"storage_key" => "storage_value"}
    end

    test "handles storage fetch error" do
      state = create_test_state(read_version: 12_345)

      storage_fetch_fn = fn :storage1_pid, "error_key", 12_345, [timeout: 100] ->
        {:error, :not_found}
      end

      opts = [storage_fetch_fn: storage_fetch_fn]

      {new_state, result} = Fetching.do_fetch(state, "error_key", opts)

      assert result == :error
      assert new_state.reads == %{"error_key" => :error}
    end

    test "acquires read version when nil" do
      state = create_test_state(read_version: nil)
      current_time = 50_000

      next_read_version_fn = fn ^state -> {:ok, 12_345, 5000} end
      time_fn = fn -> current_time end

      storage_fetch_fn = fn :storage1_pid, "key", 12_345, [timeout: 100] ->
        {:ok, "value"}
      end

      opts = [
        next_read_version_fn: next_read_version_fn,
        time_fn: time_fn,
        storage_fetch_fn: storage_fetch_fn
      ]

      {new_state, result} = Fetching.do_fetch(state, "key", opts)

      assert result == {:ok, "value"}
      assert new_state.read_version == 12_345
      assert new_state.read_version_lease_expiration == current_time + 5000
      assert new_state.reads == %{"key" => "value"}
    end

    test "handles next_read_version unavailable error" do
      state = create_test_state(read_version: nil)

      next_read_version_fn = fn ^state -> {:error, :unavailable} end

      opts = [next_read_version_fn: next_read_version_fn]

      assert_raise RuntimeError, "No read version available for fetching key: \"key\"", fn ->
        Fetching.do_fetch(state, "key", opts)
      end
    end

    test "handles key encoding error" do
      defmodule FailingKeyCodec do
        def encode_key(_), do: :key_error
      end

      state = create_test_state(key_codec: FailingKeyCodec)

      assert_raise MatchError, fn ->
        Fetching.do_fetch(state, :invalid_key)
      end
    end

    test "handles value decoding error" do
      defmodule FailingValueCodec do
        def encode_value(value), do: {:ok, value}
        def decode_value(_), do: :decode_error
      end

      state =
        create_test_state(
          read_version: 12_345,
          value_codec: FailingValueCodec
        )

      storage_fetch_fn = fn :storage1_pid, "key", 12_345, [timeout: 100] ->
        {:ok, "encoded_value"}
      end

      opts = [storage_fetch_fn: storage_fetch_fn]

      {new_state, result} = Fetching.do_fetch(state, "key", opts)

      assert result == :decode_error
      # State unchanged on decode error
      assert new_state == state
    end
  end

  describe "fetch_from_stack/2" do
    test "returns :error for empty stack" do
      result = Fetching.fetch_from_stack("key", [])
      assert result == :error
    end

    test "finds value in writes of first stack frame" do
      stack = [{%{}, %{"key" => "value_from_writes"}}]
      result = Fetching.fetch_from_stack("key", stack)
      assert result == {:ok, "value_from_writes"}
    end

    test "finds value in reads of first stack frame" do
      stack = [{%{"key" => "value_from_reads"}, %{}}]
      result = Fetching.fetch_from_stack("key", stack)
      assert result == {:ok, "value_from_reads"}
    end

    test "writes take precedence over reads in same frame" do
      stack = [{%{"key" => "read_value"}, %{"key" => "write_value"}}]
      result = Fetching.fetch_from_stack("key", stack)
      assert result == {:ok, "write_value"}
    end

    test "searches through multiple stack frames" do
      stack = [
        # Empty frame
        {%{}, %{}},
        # Frame without our key
        {%{"other_key" => "other_value"}, %{}},
        # Frame with our key
        {%{"key" => "found_value"}, %{}}
      ]

      result = Fetching.fetch_from_stack("key", stack)
      assert result == {:ok, "found_value"}
    end

    test "returns first match when key exists in multiple frames" do
      stack = [
        {%{"key" => "first_value"}, %{}},
        {%{"key" => "second_value"}, %{}}
      ]

      result = Fetching.fetch_from_stack("key", stack)
      assert result == {:ok, "first_value"}
    end
  end

  describe "storage_servers_for_key/2" do
    test "finds storage servers for key in range" do
      layout = %{
        storage_teams: [
          %{
            key_range: {"a", "m"},
            storage_ids: ["storage1", "storage2"]
          },
          %{
            key_range: {"m", :end},
            storage_ids: ["storage3"]
          }
        ],
        services: %{
          "storage1" => %{kind: :storage, status: {:up, :pid1}},
          "storage2" => %{kind: :storage, status: {:up, :pid2}},
          "storage3" => %{kind: :storage, status: {:up, :pid3}}
        }
      }

      # Key "hello" should be in first range
      result = Fetching.storage_servers_for_key(layout, "hello")
      expected = [{{"a", "m"}, :pid1}, {{"a", "m"}, :pid2}]
      assert result == expected

      # Key "zebra" should be in second range
      result = Fetching.storage_servers_for_key(layout, "zebra")
      expected = [{{"m", :end}, :pid3}]
      assert result == expected
    end

    test "filters out down storage servers" do
      layout = %{
        storage_teams: [
          %{
            key_range: {"", :end},
            storage_ids: ["storage1", "storage2", "storage3"]
          }
        ],
        services: %{
          "storage1" => %{kind: :storage, status: {:up, :pid1}},
          "storage2" => %{kind: :storage, status: {:down, nil}},
          "storage3" => %{kind: :storage, status: {:up, :pid3}}
        }
      }

      result = Fetching.storage_servers_for_key(layout, "key")
      expected = [{{"", :end}, :pid1}, {{"", :end}, :pid3}]
      assert result == expected
    end

    test "returns empty list when no servers available" do
      layout = %{
        storage_teams: [
          %{
            key_range: {"a", "b"},
            storage_ids: ["storage1"]
          }
        ],
        services: %{
          "storage1" => %{kind: :storage, status: {:up, :pid1}}
        }
      }

      # Key "z" is outside range
      result = Fetching.storage_servers_for_key(layout, "z")
      assert result == []
    end
  end

  describe "fastest_storage_server_for_key/2" do
    test "finds fastest server for key in range" do
      fastest_servers = %{
        {"a", "m"} => :fast_pid1,
        {"m", :end} => :fast_pid2
      }

      assert Fetching.fastest_storage_server_for_key(fastest_servers, "hello") == :fast_pid1
      assert Fetching.fastest_storage_server_for_key(fastest_servers, "zebra") == :fast_pid2
    end

    test "returns nil when no fastest server found" do
      fastest_servers = %{
        {"a", "m"} => :fast_pid1
      }

      assert Fetching.fastest_storage_server_for_key(fastest_servers, "zebra") == nil
    end

    test "handles :end boundary correctly" do
      fastest_servers = %{
        {"m", :end} => :fast_pid
      }

      assert Fetching.fastest_storage_server_for_key(fastest_servers, "m") == :fast_pid
      assert Fetching.fastest_storage_server_for_key(fastest_servers, "zzzz") == :fast_pid
    end
  end

  describe "horse_race_storage_servers_for_key/5" do
    test "returns :error for empty server list" do
      result = Fetching.horse_race_storage_servers_for_key([], 12_345, "key", 100, [])
      assert result == :error
    end

    test "returns first successful response" do
      servers = [
        {{"a", "m"}, :pid1},
        {{"a", "m"}, :pid2}
      ]

      async_stream_fn = fn servers, fun, _opts ->
        servers
        |> Enum.map(fun)
        |> Enum.map(&{:ok, &1})
      end

      storage_fetch_fn = fn
        :pid1, "key", 12_345, [timeout: 100] -> {:ok, "value1"}
        :pid2, "key", 12_345, [timeout: 100] -> {:error, :timeout}
      end

      opts = [async_stream_fn: async_stream_fn, storage_fetch_fn: storage_fetch_fn]

      result = Fetching.horse_race_storage_servers_for_key(servers, 12_345, "key", 100, opts)
      assert result == {:ok, {"a", "m"}, :pid1, "value1"}
    end

    test "handles all servers returning errors" do
      servers = [
        {{"a", "m"}, :pid1},
        {{"a", "m"}, :pid2}
      ]

      async_stream_fn = fn servers, fun, _opts ->
        servers
        |> Enum.map(fun)
        |> Enum.map(&{:ok, &1})
      end

      storage_fetch_fn = fn
        :pid1, "key", 12_345, [timeout: 100] -> {:error, :timeout}
        :pid2, "key", 12_345, [timeout: 100] -> {:error, :not_found}
      end

      opts = [async_stream_fn: async_stream_fn, storage_fetch_fn: storage_fetch_fn]

      result = Fetching.horse_race_storage_servers_for_key(servers, 12_345, "key", 100, opts)
      # Returns first error that matches the pattern
      assert result == {:error, :not_found}
    end

    test "prioritizes version errors" do
      servers = [
        {{"a", "m"}, :pid1},
        {{"a", "m"}, :pid2}
      ]

      async_stream_fn = fn servers, fun, _opts ->
        servers
        |> Enum.map(fun)
        |> Enum.map(&{:ok, &1})
      end

      storage_fetch_fn = fn
        :pid1, "key", 12_345, [timeout: 100] -> {:error, :timeout}
        :pid2, "key", 12_345, [timeout: 100] -> {:error, :version_too_old}
      end

      opts = [async_stream_fn: async_stream_fn, storage_fetch_fn: storage_fetch_fn]

      result = Fetching.horse_race_storage_servers_for_key(servers, 12_345, "key", 100, opts)
      assert result == {:error, :version_too_old}
    end
  end

  describe "fetch_from_fastest_storage_server/4" do
    test "updates fastest_storage_servers when successful" do
      state = create_test_state(read_version: 12_345, fastest_storage_servers: %{})
      servers = [{{"a", "m"}, :pid1}]

      horse_race_fn = fn ^servers, 12_345, "key", 100, _opts ->
        {:ok, {"a", "m"}, :pid1, "value"}
      end

      opts = [horse_race_fn: horse_race_fn]

      {:ok, new_state, value} =
        Fetching.fetch_from_fastest_storage_server(state, servers, "key", opts)

      assert value == "value"
      assert new_state.fastest_storage_servers == %{{"a", "m"} => :pid1}
    end

    test "propagates horse race errors" do
      state = create_test_state(read_version: 12_345)
      servers = [{{"a", "m"}, :pid1}]

      horse_race_fn = fn ^servers, 12_345, "key", 100, _opts ->
        {:error, :timeout}
      end

      opts = [horse_race_fn: horse_race_fn]

      result = Fetching.fetch_from_fastest_storage_server(state, servers, "key", opts)
      assert result == {:error, :timeout}
    end
  end

  describe "integration scenarios" do
    test "complete fetch flow with storage fallback" do
      # Start with empty caches, should go to storage
      state = create_test_state(read_version: 12_345)

      storage_fetch_fn = fn :storage1_pid, "integration_key", 12_345, [timeout: 100] ->
        {:ok, "integration_value"}
      end

      opts = [storage_fetch_fn: storage_fetch_fn]

      {new_state, result} = Fetching.do_fetch(state, "integration_key", opts)

      assert result == {:ok, "integration_value"}
      assert new_state.reads == %{"integration_key" => "integration_value"}

      # Second fetch should hit reads cache
      {final_state, result2} = Fetching.do_fetch(new_state, "integration_key", opts)

      assert result2 == {:ok, "integration_value"}
      # No change since it hit cache
      assert final_state == new_state
    end

    test "fetch with stack fallback" do
      # Key not in current reads/writes but in stack
      stack = [{%{"stack_key" => "stack_value"}, %{}}]
      state = create_test_state(stack: stack)

      {new_state, result} = Fetching.do_fetch(state, "stack_key")

      assert result == {:ok, "stack_value"}
      # No change when reading from stack
      assert new_state == state
    end
  end
end
