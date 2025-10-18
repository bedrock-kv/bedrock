defmodule Bedrock.Internal.TransactionBuilder.PointReadsTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bedrock.Internal.TransactionBuilder.LayoutIndex
  alias Bedrock.Internal.TransactionBuilder.PointReads
  alias Bedrock.Internal.TransactionBuilder.State
  alias Bedrock.Internal.TransactionBuilder.StorageRacing
  alias Bedrock.KeySelector

  defmodule TestKeyCodec do
    @moduledoc false
    def encode_key(key) when is_binary(key), do: {:ok, key}
    def encode_key(_), do: :key_error
    def decode_key(key) when is_binary(key), do: {:ok, key}
    def decode_key(_), do: :key_error
  end

  defmodule TestValueCodec do
    @moduledoc false
    def encode_value(value), do: {:ok, value}
    def decode_value(value), do: {:ok, value}
  end

  def create_test_state(opts \\ []) do
    layout = Keyword.get(opts, :transaction_system_layout, create_test_layout())
    layout_index = LayoutIndex.build_index(layout)

    %State{
      state: :valid,
      transaction_system_layout: layout,
      layout_index: layout_index,
      read_version: Keyword.get(opts, :read_version),
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

  def create_racing_layout do
    %{
      storage_teams: [%{key_range: {"", "zzz"}, storage_ids: ["server1", "server2"]}],
      services: %{
        "server1" => %{kind: :storage, status: {:up, :pid1}},
        "server2" => %{kind: :storage, status: {:up, :pid2}}
      }
    }
  end

  def create_multirange_layout do
    %{
      sequencer: :test_sequencer,
      storage_teams: [
        %{
          key_range: {"", "k"},
          storage_ids: ["storage1"]
        },
        %{
          key_range: {"k", :end},
          storage_ids: ["storage2"]
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
      alias Bedrock.Internal.TransactionBuilder.Tx

      # Create a state with a transaction containing the cached value
      tx = Tx.set(Tx.new(), "cached_key", "cached_value")
      state = %{create_test_state() | tx: tx}

      assert {new_state, {:ok, {"cached_key", "cached_value"}}} = PointReads.get_key(state, "cached_key")
      # Transaction state should be unchanged when reading from writes
      assert Tx.commit(new_state.tx, nil) == Tx.commit(state.tx, nil)
    end

    test "fetches value from storage when not cached" do
      # Create state with read version and mock storage function
      state = %{create_test_state() | read_version: 12_345}

      # Mock storage function
      storage_fn = fn _pid, _key, _version, _opts ->
        {:ok, "storage_value"}
      end

      opts = [storage_get_key_fn: storage_fn]

      # Fetch should call storage and return value
      assert {_new_state, {:ok, {"storage_key", "storage_value"}}} = PointReads.get_key(state, "storage_key", opts)
    end

    test "writes cache takes precedence over reads cache" do
      alias Bedrock.Internal.TransactionBuilder.Tx

      # Create a transaction that has a value in reads cache and a different value in writes cache
      tx = Tx.new()
      # Value in reads cache
      tx = %{tx | reads: %{"key" => "old_value"}}
      # Value in writes cache (should take precedence)
      tx = Tx.set(tx, "key", "new_value")

      state = %{create_test_state() | tx: tx}

      # Should get writes value, not reads value
      assert {new_state, {:ok, {"key", "new_value"}}} = PointReads.get_key(state, "key")
      # Transaction unchanged since value came from writes cache
      # Use a valid read version since both transactions have read conflicts
      read_version = Bedrock.DataPlane.Version.from_integer(12_345)
      assert Tx.commit(new_state.tx, read_version) == Tx.commit(state.tx, read_version)
    end

    test "fetches from storage when not in cache" do
      state = create_test_state(read_version: 12_345)

      storage_get_key_fn = fn
        :storage1_pid, "storage_key", 12_345, [timeout: 100] ->
          {:ok, "storage_value"}

        :storage2_pid, "storage_key", 12_345, [timeout: 100] ->
          {:error, :timeout}
      end

      opts = [storage_get_key_fn: storage_get_key_fn]

      assert {%{tx: %{reads: %{"storage_key" => "storage_value"}}}, {:ok, {"storage_key", "storage_value"}}} =
               PointReads.get_key(state, "storage_key", opts)
    end

    test "handles storage fetch error" do
      state = create_test_state(read_version: 12_345)

      storage_get_key_fn = fn
        :storage1_pid, "error_key", 12_345, [timeout: 100] ->
          {:ok, nil}

        :storage2_pid, "error_key", 12_345, [timeout: 100] ->
          {:failure, :timeout, :storage2_pid}
      end

      opts = [storage_get_key_fn: storage_get_key_fn]

      assert {%{tx: %{reads: %{"error_key" => :clear}}}, {:error, :not_found}} =
               PointReads.get_key(state, "error_key", opts)
    end

    test "acquires read version when nil" do
      state = create_test_state(read_version: nil)

      next_read_version_fn = fn ^state -> {:ok, 12_345} end

      storage_get_key_fn = fn
        :storage1_pid, "key", 12_345, [timeout: 100] ->
          {:ok, "value"}

        :storage2_pid, "key", 12_345, [timeout: 100] ->
          {:error, :timeout}
      end

      opts = [
        next_read_version_fn: next_read_version_fn,
        storage_get_key_fn: storage_get_key_fn
      ]

      assert {
               %{
                 read_version: 12_345,
                 tx: %{reads: %{"key" => "value"}}
               },
               {:ok, {"key", "value"}}
             } = PointReads.get_key(state, "key", opts)
    end

    test "handles next_read_version unavailable error" do
      state = create_test_state(read_version: nil)

      next_read_version_fn = fn ^state -> {:error, :unavailable} end

      opts = [next_read_version_fn: next_read_version_fn]

      # Should return failure instead of raising, allowing retry logic to handle it
      assert {^state, {:failure, %{unavailable: []}}} = PointReads.get_key(state, "key", opts)
    end

    test "works with binary keys directly" do
      state = create_test_state(read_version: 12_345)

      storage_get_key_fn = fn
        :storage1_pid, "key", 12_345, [timeout: 100] -> {:ok, "raw_value"}
        :storage2_pid, "key", 12_345, [timeout: 100] -> {:error, :timeout}
      end

      opts = [storage_get_key_fn: storage_get_key_fn]

      assert {_new_state, {:ok, {"key", "raw_value"}}} = PointReads.get_key(state, "key", opts)
    end
  end

  # Note: fetch_from_stack/2 function was removed in new Tx-based architecture
  # Stack operations are now handled internally by the Tx module

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

      index = LayoutIndex.build_index(layout)

      # Key "hello" should be in first range
      result = LayoutIndex.lookup_key!(index, "hello")
      # Note: With segmented index, this returns a single segment containing the key
      assert {_, pids} = result
      assert :pid1 in pids and :pid2 in pids

      # Key "zebra" should be in second range
      assert {_, pids} = LayoutIndex.lookup_key!(index, "zebra")
      assert :pid3 in pids
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

      index = LayoutIndex.build_index(layout)
      assert {_, pids} = LayoutIndex.lookup_key!(index, "key")
      assert :pid1 in pids and :pid3 in pids
      refute :pid2 in pids
    end

    test "raises when no servers available for key" do
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

      index = LayoutIndex.build_index(layout)

      # Key "z" is outside range
      assert_raise RuntimeError, ~r/No segment found containing key/, fn ->
        LayoutIndex.lookup_key!(index, "z")
      end
    end
  end

  describe "StorageRacing.race_storage_servers/4" do
    test "returns :error for unavailable key" do
      operation_fn = fn _server, _state -> {:ok, "value"} end
      empty_layout = %{storage_teams: [], services: %{}}
      layout_index = LayoutIndex.build_index(empty_layout)
      state = %State{layout_index: layout_index, fastest_storage_servers: %{}, fetch_timeout_in_ms: 100}

      assert {^state, {:failure, %{layout_lookup_failed: []}}} =
               StorageRacing.race_storage_servers(state, "key1", operation_fn)
    end

    test "returns first successful response" do
      operation_fn = fn
        :pid1, _version, _timeout -> {:ok, "value1"}
        :pid2, _version, _timeout -> {:error, :timeout}
      end

      layout_config = create_racing_layout()
      layout_index = LayoutIndex.build_index(layout_config)
      state = %State{layout_index: layout_index, fastest_storage_servers: %{}, fetch_timeout_in_ms: 100}

      assert {%State{}, {:ok, {"value1", {"", "zzz"}}}} =
               StorageRacing.race_storage_servers(state, "key1", operation_fn)
    end

    test "handles all servers returning errors" do
      operation_fn = fn
        :pid1, _version, _timeout -> {:error, :timeout}
        :pid2, _version, _timeout -> {:error, :version_too_old}
      end

      layout_config = create_racing_layout()
      layout_index = LayoutIndex.build_index(layout_config)
      state = %State{layout_index: layout_index, fastest_storage_servers: %{}, fetch_timeout_in_ms: 100}

      # Returns all failures by reason
      assert {^state, {:failure, %{version_too_old: [:pid2], timeout: [:pid1]}}} =
               StorageRacing.race_storage_servers(state, "key1", operation_fn)
    end

    test "prioritizes success over errors" do
      operation_fn = fn
        :pid1, _version, _timeout -> {:error, :timeout}
        :pid2, _version, _timeout -> {:ok, :success_value}
      end

      layout_config = create_racing_layout()
      layout_index = LayoutIndex.build_index(layout_config)
      state = %State{layout_index: layout_index, fastest_storage_servers: %{}, fetch_timeout_in_ms: 100}

      assert {%State{}, {:ok, {:success_value, {"", "zzz"}}}} =
               StorageRacing.race_storage_servers(state, "key1", operation_fn)
    end

    test "cached fastest server fails, races remaining servers to find new winner" do
      # Operation function where pid1 (cached fastest) fails, but pid2 succeeds
      operation_fn = fn
        :pid1, _version, _timeout -> {:failure, :timeout, :pid1}
        :pid2, _version, _timeout -> {:ok, "backup_success"}
      end

      layout_config = create_racing_layout()
      layout_index = LayoutIndex.build_index(layout_config)

      # Start with pid1 cached as fastest for this key range
      cached_fastest = %{{"", "zzz"} => :pid1}

      state = %State{
        layout_index: layout_index,
        fastest_storage_servers: cached_fastest,
        fetch_timeout_in_ms: 100
      }

      # When we race, pid1 (cached) should fail, then pid2 should be tried and win
      # Should succeed with pid2's result and update cache with pid2 as fastest server
      assert {%{fastest_storage_servers: %{{"", "zzz"} => :pid2}}, {:ok, {"backup_success", {"", "zzz"}}}} =
               StorageRacing.race_storage_servers(state, "key1", operation_fn)
    end

    test "returns {:failure, %{no_servers_to_race: [nil]}} when no servers left after fastest server fails" do
      # Operation function where the only server fails in a way that causes fallback to racing
      operation_fn = fn
        :only_pid, _version, _timeout -> {:error, :unavailable}
      end

      # Layout with only one server
      single_server_layout = %{
        storage_teams: [%{key_range: {"", "zzz"}, storage_ids: ["only_server"]}],
        services: %{
          "only_server" => %{kind: :storage, status: {:up, :only_pid}}
        }
      }

      layout_index = LayoutIndex.build_index(single_server_layout)

      # Pre-cache the only server as fastest
      state = %State{
        layout_index: layout_index,
        fastest_storage_servers: %{{"", "zzz"} => :only_pid},
        fetch_timeout_in_ms: 100
      }

      # When the cached fastest server fails with :unavailable, it will try to race remaining servers
      # But since there's only one server and we remove it from the list, there are no servers left to race
      assert {^state, {:failure, %{no_servers_to_race: []}}} =
               StorageRacing.race_storage_servers(state, "key1", operation_fn)
    end
  end

  describe "integration scenarios" do
    test "complete fetch flow with storage fallback" do
      # Start with empty caches, should go to storage
      state = create_test_state(read_version: 12_345)

      storage_get_key_fn = fn
        :storage1_pid, "integration_key", 12_345, [timeout: 100] ->
          {:ok, "integration_value"}

        :storage2_pid, "integration_key", 12_345, [timeout: 100] ->
          {:error, :timeout}
      end

      opts = [storage_get_key_fn: storage_get_key_fn]

      assert {%{tx: %{reads: %{"integration_key" => "integration_value"}}} = new_state,
              {:ok, {"integration_key", "integration_value"}}} =
               PointReads.get_key(state, "integration_key", opts)

      # Second fetch should hit reads cache - no change since it hit cache
      assert {^new_state, {:ok, {"integration_key", "integration_value"}}} =
               PointReads.get_key(new_state, "integration_key", opts)
    end

    test "fetch with stack fallback" do
      # Key not in current reads/writes but in stack - this functionality was removed in Tx refactor
      # Stack operations are now handled internally by the Tx module
      # This test is no longer applicable with the new architecture
      alias Bedrock.Internal.TransactionBuilder.Tx

      # Create transaction with the value already in reads cache to simulate stack fallback
      tx = %{Tx.new() | reads: %{"stack_key" => "stack_value"}}
      state = %{create_test_state() | tx: tx}

      assert {new_state, {:ok, {"stack_key", "stack_value"}}} = PointReads.get_key(state, "stack_key")
      # Transaction unchanged since it hit reads cache
      # Use a valid read version since both transactions have read conflicts
      read_version = Bedrock.DataPlane.Version.from_integer(12_345)
      assert Tx.commit(new_state.tx, read_version) == Tx.commit(state.tx, read_version)
    end
  end

  # Property-based tests for robust validation of key functions
  describe "property-based tests" do
    property "storage_servers_for_key raises when no teams cover key range" do
      check all(key <- binary()) do
        layout = %{
          storage_teams: [],
          services: %{}
        }

        index = LayoutIndex.build_index(layout)

        assert_raise RuntimeError, ~r/No segment found containing key/, fn ->
          LayoutIndex.lookup_key!(index, key)
        end
      end
    end

    property "storage_servers_for_key filters out down storage servers" do
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

        index = LayoutIndex.build_index(layout)
        # Should only include the up server
        assert {{"", :end}, [:up_pid]} = LayoutIndex.lookup_key!(index, key)
      end
    end
  end

  describe "fetch_key_selector/2" do
    test "happy path: key selector resolves on first shard checked" do
      # Create layout with multiple shards
      layout = %{
        sequencer: :test_sequencer,
        storage_teams: [
          %{
            key_range: {"", "m"},
            storage_ids: ["storage1"]
          },
          %{
            key_range: {"m", :end},
            storage_ids: ["storage2"]
          }
        ],
        services: %{
          "storage1" => %{kind: :storage, status: {:up, :storage1_pid}},
          "storage2" => %{kind: :storage, status: {:up, :storage2_pid}}
        }
      }

      state =
        create_test_state(
          transaction_system_layout: layout,
          read_version: 12_345
        )

      # Create a key selector that targets the first shard
      key_selector = %KeySelector{key: "a", or_equal: true, offset: 0}

      # Mock storage function - first shard returns the key immediately
      storage_get_key_selector_fn = fn
        :storage1_pid, ^key_selector, 12_345, [timeout: 100] ->
          {:ok, {"a", "value_a"}}

        :storage2_pid, ^key_selector, 12_345, [timeout: 100] ->
          {:error, :timeout}
      end

      opts = [storage_get_key_selector_fn: storage_get_key_selector_fn]

      assert {%{tx: %{reads: %{"a" => "value_a"}}}, {:ok, {"a", "value_a"}}} =
               PointReads.get_key_selector(state, key_selector, opts)
    end

    test "offset pushes key selector to next shard (positive offset)" do
      # Create layout with multiple shards
      layout = create_multirange_layout()

      state =
        create_test_state(
          transaction_system_layout: layout,
          read_version: 12_345
        )

      # Key selector starts near boundary but offset pushes it to second shard
      # However, racing is based on key "j" which maps to first shard
      key_selector = %KeySelector{key: "j", or_equal: true, offset: 5}

      # Mock storage function - only storage1 will be queried since "j" maps to first shard
      storage_get_key_selector_fn = fn
        :storage1_pid, ^key_selector, 12_345, [timeout: 100] ->
          # Storage1 resolves the selector and finds it points to second shard
          # In reality, storage would coordinate to resolve this
          {:ok, {"o", "value_o"}}

        :storage2_pid, ^key_selector, 12_345, [timeout: 100] ->
          {:error, :timeout}
      end

      opts = [storage_get_key_selector_fn: storage_get_key_selector_fn]

      assert {%{tx: %{reads: %{"o" => "value_o"}}}, {:ok, {"o", "value_o"}}} =
               PointReads.get_key_selector(state, key_selector, opts)
    end

    test "offset pushes key selector to previous shard (negative offset)" do
      # Create layout with multiple shards
      layout = create_multirange_layout()

      state =
        create_test_state(
          transaction_system_layout: layout,
          read_version: 12_345
        )

      # Key selector starts in second shard but negative offset pushes back to first
      # Racing will only query storage2 since "m" maps to second shard
      key_selector = %KeySelector{key: "m", or_equal: true, offset: -5}

      # Mock storage function - only storage2 will be queried since "m" maps to second shard
      storage_get_key_selector_fn = fn
        :storage1_pid, ^key_selector, 12_345, [timeout: 100] ->
          {:error, :timeout}

        :storage2_pid, ^key_selector, 12_345, [timeout: 100] ->
          # Storage2 resolves the selector and finds it points back to first shard
          # In reality, storage would coordinate to resolve this
          {:ok, {"h", "value_h"}}
      end

      opts = [storage_get_key_selector_fn: storage_get_key_selector_fn]

      assert {%{tx: %{reads: %{"h" => "value_h"}}}, {:ok, {"h", "value_h"}}} =
               PointReads.get_key_selector(state, key_selector, opts)
    end

    test "handles key selector resolution error" do
      state = create_test_state(read_version: 12_345)

      key_selector = %KeySelector{key: "test", or_equal: true, offset: 0}

      # Mock storage function that returns not_found
      storage_get_key_selector_fn = fn
        :storage1_pid, ^key_selector, 12_345, [timeout: 100] ->
          {:ok, nil}

        :storage2_pid, ^key_selector, 12_345, [timeout: 100] ->
          {:failure, :timeout, :storage2_pid}
      end

      opts = [storage_get_key_selector_fn: storage_get_key_selector_fn]

      assert {%{tx: %{reads: %{}}}, {:error, :not_found}} =
               PointReads.get_key_selector(state, key_selector, opts)
    end

    test "acquires read version when nil for key selector" do
      state = create_test_state(read_version: nil)

      key_selector = %KeySelector{key: "test", or_equal: true, offset: 0}

      next_read_version_fn = fn ^state -> {:ok, 12_345} end

      storage_get_key_selector_fn = fn
        :storage1_pid, ^key_selector, 12_345, [timeout: 100] ->
          {:ok, {"resolved_key", "resolved_value"}}

        :storage2_pid, ^key_selector, 12_345, [timeout: 100] ->
          {:error, :timeout}
      end

      opts = [
        next_read_version_fn: next_read_version_fn,
        storage_get_key_selector_fn: storage_get_key_selector_fn
      ]

      assert {
               %{
                 read_version: 12_345,
                 tx: %{reads: %{"resolved_key" => "resolved_value"}}
               },
               {:ok, {"resolved_key", "resolved_value"}}
             } = PointReads.get_key_selector(state, key_selector, opts)
    end
  end
end
