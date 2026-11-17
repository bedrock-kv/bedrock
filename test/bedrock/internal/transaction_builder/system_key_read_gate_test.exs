defmodule Bedrock.Internal.TransactionBuilder.SystemKeyReadGateTest do
  @moduledoc """
  Pins the client-side read bound (bedrock-q67.26).

  FoundationDB bounds reads in the client transaction object, not at a
  server: `ReadYourWritesTransaction` compares every read address against
  `getMaxReadKey()` — `normalKeys.end` (`\\xFF`) normally, `systemKeys.end`
  (`\\xFF\\xFF`) once `READ_SYSTEM_KEYS` is set — and throws
  `key_outside_legal_range` before any location lookup
  (ReadYourWrites.actor.cpp:1682, 1704, 1741). Bedrock's transaction
  builder is that object, so the bound lives here, ahead of routing.

  The routing fn reports every key it is asked for: a gated read must never
  reach it.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.TransactionBuilder
  alias Bedrock.KeySelector

  @boundary Bedrock.end_of_user_keyspace()
  @end_of_keyspace Bedrock.end_of_keyspace()

  # Reads need a read version and a routing answer; both are supplied
  # in-process so a read that passes the bound fails with a shape no gate
  # could produce.
  defp read_opts, do: [next_read_version_fn: fn _t -> {:ok, Version.from_integer(1)} end]

  defp start_builder(opts) do
    test_pid = self()

    routing_fn = fn key ->
      send(test_pid, {:routing_asked, key})
      {:error, :unavailable}
    end

    start_supervised!(
      {TransactionBuilder,
       Keyword.merge(
         [transaction_system_layout: %{epoch: 1, proxies: []}, routing_fn: routing_fn],
         opts
       )}
    )
  end

  describe "reads without the system-read option" do
    setup do
      {:ok, builder: start_builder([])}
    end

    test "a value read at the boundary is rejected before routing is asked", %{builder: builder} do
      assert {:error, {:key_out_of_range, @boundary}} =
               GenServer.call(builder, {:get, @boundary, read_opts()})

      refute_received {:routing_asked, _}
    end

    test "a value read inside the system keyspace is rejected, naming the key", %{builder: builder} do
      key = <<0xFF, "/system/config/desired_commit_proxies">>

      assert {:error, {:key_out_of_range, ^key}} = GenServer.call(builder, {:get, key, read_opts()})

      refute_received {:routing_asked, _}
    end

    test "the highest user key still reaches routing", %{builder: builder} do
      key = <<0xFE, 0xFF, 0xFF, 0xFF>>

      assert {:failure, :unavailable} = GenServer.call(builder, {:get, key, read_opts()})

      assert_received {:routing_asked, ^key}
    end

    test "a range read past the boundary is rejected at whichever end escapes", %{builder: builder} do
      assert {:error, {:key_out_of_range, <<0xFF, 0x00>>}} =
               GenServer.call(builder, {:get_range, "a", <<0xFF, 0x00>>, 10, read_opts()})

      assert {:error, {:key_out_of_range, <<0xFF, 0x01>>}} =
               GenServer.call(builder, {:get_range, <<0xFF, 0x01>>, @end_of_keyspace, 10, read_opts()})

      refute_received {:routing_asked, _}
    end

    test "a range read ending exactly AT the boundary is legal", %{builder: builder} do
      # The end is exclusive, so `\xFF` is how a scan reaches the last user
      # key - the same reason a clear_range may end there.
      assert {:failure, :unavailable} = GenServer.call(builder, {:get_range, "a", @boundary, 10, read_opts()})

      assert_received {:routing_asked, "a"}
    end

    test "a key selector anchored past the boundary is rejected", %{builder: builder} do
      key = <<0xFF, "/system/shard/">>
      selector = KeySelector.first_greater_or_equal(key)

      assert {:error, {:key_out_of_range, ^key}} =
               GenServer.call(builder, {:get_key_selector, selector, read_opts()})

      refute_received {:routing_asked, _}
    end

    test "a key selector anchored AT the boundary is legal", %{builder: builder} do
      # firstGreaterOrEqual(\xFF) is how a selector addresses the end of the
      # user keyspace; the anchor is a bound, not a key being read.
      selector = KeySelector.first_greater_or_equal(@boundary)

      assert {:failure, :unavailable} = GenServer.call(builder, {:get_key_selector, selector, read_opts()})

      assert_received {:routing_asked, @boundary}
    end

    test "a selector range is rejected on either escaping endpoint", %{builder: builder} do
      inside = KeySelector.first_greater_or_equal("a")
      outside = KeySelector.first_greater_or_equal(<<0xFF, 0x00>>)

      assert {:error, {:key_out_of_range, <<0xFF, 0x00>>}} =
               GenServer.call(builder, {:get_range_selectors, inside, outside, read_opts()})

      assert {:error, {:key_out_of_range, <<0xFF, 0x00>>}} =
               GenServer.call(builder, {:get_range_selectors, outside, inside, read_opts()})

      refute_received {:routing_asked, _}
    end
  end

  describe "reads with read_system_keys: true" do
    setup do
      {:ok, builder: start_builder(read_system_keys: true)}
    end

    test "the same system read reaches routing", %{builder: builder} do
      key = <<0xFF, "/system/config/desired_commit_proxies">>

      assert {:failure, :unavailable} = GenServer.call(builder, {:get, key, read_opts()})

      assert_received {:routing_asked, ^key}
    end

    test "the bound moves to the end of the keyspace, it does not vanish", %{builder: builder} do
      # Privatized keys live past end_of_keyspace/0 and belong to no shard;
      # the system-read option admits `\xFF`, never `\xFF\xFF`.
      assert {:error, {:key_out_of_range, @end_of_keyspace}} =
               GenServer.call(builder, {:get, @end_of_keyspace, read_opts()})

      past = @end_of_keyspace <> "/materializers/7"

      assert {:error, {:key_out_of_range, ^past}} =
               GenServer.call(builder, {:get_range, <<0xFF>>, past, 10, read_opts()})

      refute_received {:routing_asked, _}
    end
  end
end
