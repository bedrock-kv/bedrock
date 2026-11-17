defmodule Bedrock.Internal.TransactionBuilder.StorageRacingTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.TransactionBuilder.LayoutIndex
  alias Bedrock.Internal.TransactionBuilder.State
  alias Bedrock.Internal.TransactionBuilder.StorageRacing

  defp state_for(storage_refs) do
    %State{
      layout_index: %LayoutIndex{
        tree: :gb_trees.from_orddict([{<<0xFF, 0xFF>>, {"", storage_refs}}])
      },
      read_version: Version.from_integer(42),
      fetch_timeout_in_ms: 50
    }
  end

  describe "cached-fastest server failures" do
    test "a slow singleton server fails as :timeout, not a routing-shaped verdict" do
      server = self()
      state = state_for([server])

      {state, {:ok, {"value", _range}}} =
        StorageRacing.race_storage_servers(state, "key", fn ^server, _version, _timeout -> {:ok, "value"} end)

      assert %{{"", <<0xFF, 0xFF>>} => ^server} = state.fastest_storage_servers

      # The second read of the transaction takes the cached-fastest path.
      # A merely slow server must not degrade into :no_servers_to_race,
      # which would evict the node's routing cache.
      assert {_state, {:failure, %{timeout: [^server]}}} =
               StorageRacing.race_storage_servers(state, "key", fn ^server, _version, _timeout ->
                 {:failure, :timeout, server}
               end)
    end

    test "a dead singleton server still fails as :unavailable" do
      server = self()
      state = %{state_for([server]) | fastest_storage_servers: %{{"", <<0xFF, 0xFF>>} => server}}

      assert {_state, {:failure, %{unavailable: [^server]}}} =
               StorageRacing.race_storage_servers(state, "key", fn ^server, _version, _timeout ->
                 {:failure, :unavailable, server}
               end)
    end

    test "a version_too_new reply on a singleton server fails as :version_too_new" do
      server = self()
      state = %{state_for([server]) | fastest_storage_servers: %{{"", <<0xFF, 0xFF>>} => server}}

      assert {_state, {:failure, %{version_too_new: [^server]}}} =
               StorageRacing.race_storage_servers(state, "key", fn ^server, _version, _timeout ->
                 {:error, :version_too_new}
               end)
    end

    test "a slow server still races the shard's other servers" do
      slow = self()
      other = spawn(fn -> :ok end)
      state = %{state_for([slow, other]) | fastest_storage_servers: %{{"", <<0xFF, 0xFF>>} => slow}}

      assert {state, {:ok, {"value", _range}}} =
               StorageRacing.race_storage_servers(state, "key", fn
                 ^slow, _version, _timeout -> {:failure, :timeout, slow}
                 ^other, _version, _timeout -> {:ok, "value"}
               end)

      assert %{{"", <<0xFF, 0xFF>>} => ^other} = state.fastest_storage_servers
    end

    test "every other server failing reports their reasons, not :no_servers_to_race" do
      slow = self()
      other = spawn(fn -> :ok end)
      state = %{state_for([slow, other]) | fastest_storage_servers: %{{"", <<0xFF, 0xFF>>} => slow}}

      assert {_state, {:failure, %{unavailable: [^other]}}} =
               StorageRacing.race_storage_servers(state, "key", fn
                 ^slow, _version, _timeout -> {:failure, :timeout, slow}
                 ^other, _version, _timeout -> {:failure, :unavailable, other}
               end)
    end
  end

  test "a shard with no servers is :no_servers_to_race" do
    state = state_for([])

    assert {_state, {:failure, %{no_servers_to_race: []}}} =
             StorageRacing.race_storage_servers(state, "key", fn _server, _version, _timeout -> {:ok, "value"} end)
  end
end
