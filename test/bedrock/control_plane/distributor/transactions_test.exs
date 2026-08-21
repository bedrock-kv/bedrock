defmodule Bedrock.ControlPlane.Distributor.TransactionsTest do
  @moduledoc """
  The distributor's fenced system transactions. Every mutating commit
  carries read conflicts on the lock keys at the version the fence was
  evaluated, so a concurrent take conflicts with it — ownership enforced
  by the commit pipeline, not by supervision.
  """
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Distributor.Lock
  alias Bedrock.ControlPlane.Distributor.Transactions
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.SystemKeys

  defp deps(overrides) do
    v = Version.from_integer(100)

    Map.merge(
      %{
        epoch: 7,
        proxies: [:proxy_a],
        next_read_version_fn: fn -> {:ok, v} end,
        get_fn: fn _key, _version -> {:error, :not_found} end,
        commit_fn: fn _proxy, _epoch, _encoded, _opts -> {:ok, v, 0} end,
        get_range_fn: fn _start, _end, _version -> {:ok, {[], false}} end
      },
      overrides
    )
  end

  describe "take_lock/1" do
    test "reads both keys, claims the owner, and fences the commit with read conflicts at the read version" do
      test_pid = self()
      v = Version.from_integer(42)

      deps =
        deps(%{
          next_read_version_fn: fn -> {:ok, v} end,
          get_fn: fn key, version ->
            send(test_pid, {:read, key, version})
            {:error, :not_found}
          end,
          commit_fn: fn proxy, epoch, encoded, opts ->
            send(test_pid, {:committed, proxy, epoch, encoded, opts})
            {:ok, v, 0}
          end
        })

      assert {:ok, %Lock{prev_owner: nil, prev_write: nil} = lock} = Transactions.take_lock(deps)

      owner_key = SystemKeys.distributor_lock_owner()
      write_key = SystemKeys.distributor_lock_write()

      # FDB's takeMoveKeysLock reads BOTH keys (the remembered write is
      # the unobserved-take evidence), at the fence's read version.
      assert_received {:read, ^owner_key, ^v}
      assert_received {:read, ^write_key, ^v}

      assert_received {:committed, :proxy_a, 7, encoded, opts}
      assert Keyword.get(opts, :mode) == :system

      # The claim itself.
      assert {:ok, mutations} = Transaction.mutations(encoded)
      assert Enum.to_list(mutations) == [{:set, owner_key, lock.my_owner}]

      # The fence: read conflicts on both lock keys, so any interleaved
      # take/check conflicts with this commit.
      assert {:ok, {read_version, conflict_ranges}} = Transaction.read_conflicts(encoded)
      assert read_version == v
      covered = fn key -> Enum.any?(conflict_ranges, fn {s, e} -> key >= s and key < e end) end
      assert covered.(owner_key)
      assert covered.(write_key)
    end

    test "an existing owner's UIDs are remembered as the previous state" do
      prev_owner = Lock.new_uid()
      prev_write = Lock.new_uid()

      deps =
        deps(%{
          get_fn: fn key, _version ->
            cond do
              key == SystemKeys.distributor_lock_owner() -> {:ok, prev_owner}
              key == SystemKeys.distributor_lock_write() -> {:ok, prev_write}
            end
          end
        })

      assert {:ok, %Lock{prev_owner: ^prev_owner, prev_write: ^prev_write}} = Transactions.take_lock(deps)
    end

    test "a commit abort re-takes with a fresh read version — take is last-take-wins, not a verdict" do
      # An abort at take time can be a genuine race OR the read version
      # falling below the resolver's pruning floor; only the CHECK fence
      # and the poll deliver supersession. FDB's takeMoveKeysLock
      # retries the same way.
      test_pid = self()
      counter = :counters.new(1, [])
      winner = Lock.new_uid()

      deps =
        deps(%{
          get_fn: fn key, _version ->
            # The re-take observes the interleaved winner as previous.
            if :counters.get(counter, 1) > 0 and String.ends_with?(key, "owner"),
              do: {:ok, winner},
              else: {:error, :not_found}
          end,
          commit_fn: fn _proxy, _epoch, encoded, _opts ->
            send(test_pid, {:commit_attempt, encoded})

            if :counters.get(counter, 1) == 0 do
              :counters.add(counter, 1, 1)
              {:error, :aborted}
            else
              {:ok, Version.from_integer(9), 0}
            end
          end
        })

      assert {:ok, %Lock{prev_owner: ^winner}} = Transactions.take_lock(deps)
      assert_received {:commit_attempt, _first}
      assert_received {:commit_attempt, _second}
    end

    test "exhausted take retries surface as a transient commit failure — the director recruits again" do
      deps = deps(%{commit_fn: fn _proxy, _epoch, _encoded, _opts -> {:error, :aborted} end})

      assert {:error, {:lock_commit_failed, :aborted}} = Transactions.take_lock(deps)
    end

    test "a catching-up materializer's waitlist expiry is a transient read failure" do
      deps = deps(%{get_fn: fn _k, _v -> {:error, :waiting_timeout} end})

      assert {:error, {:lock_read_failed, :waiting_timeout}} = Transactions.take_lock(deps)
    end

    test "an unroutable system key is a read failure, never key-absence" do
      deps = deps(%{get_fn: fn key, _v -> {:error, {:unroutable_system_key, key}} end})

      assert {:error, {:lock_read_failed, {:unroutable_system_key, _key}}} = Transactions.take_lock(deps)
    end

    test "read and commit failures surface as themselves — transient, not verdicts" do
      assert {:error, {:lock_read_failed, :unavailable}} =
               Transactions.take_lock(deps(%{get_fn: fn _k, _v -> {:failure, :unavailable, :ref} end}))

      assert {:error, {:lock_commit_failed, :timeout}} =
               Transactions.take_lock(deps(%{commit_fn: fn _p, _e, _t, _o -> {:error, :timeout} end}))

      assert {:error, {:read_version_failed, :unavailable}} =
               Transactions.take_lock(deps(%{next_read_version_fn: fn -> {:error, :unavailable} end}))
    end
  end

  describe "commit_checked/3" do
    defp taken_lock do
      {lock, _mutations} = Lock.take(nil, nil)
      lock
    end

    test "steady state: reads the owner only, conflicts on the owner only, touches write + payload" do
      test_pid = self()
      lock = taken_lock()

      deps =
        deps(%{
          get_fn: fn key, _v ->
            send(test_pid, {:read, key})
            if String.ends_with?(key, "owner"), do: {:ok, lock.my_owner}, else: {:error, :not_found}
          end,
          commit_fn: fn _p, _e, encoded, _o ->
            send(test_pid, {:committed, encoded})
            {:ok, Version.from_integer(5), 0}
          end
        })

      payload = {:set, SystemKeys.materializer_key(7), "payload"}
      assert :ok = Transactions.commit_checked(lock, deps, [payload])

      owner_key = SystemKeys.distributor_lock_owner()
      write_key = SystemKeys.distributor_lock_write()

      # The runner obligation: the write key is NOT read in the
      # steady-state branch — an unconditional read would serialize all
      # concurrent same-owner distributor transactions.
      assert_received {:read, ^owner_key}
      refute_received {:read, ^write_key}

      assert_received {:committed, encoded}
      assert {:ok, {_v, conflict_ranges}} = Transaction.read_conflicts(encoded)
      covered = fn key -> Enum.any?(conflict_ranges, fn {s, e} -> key >= s and key < e end) end
      assert covered.(owner_key)
      refute covered.(write_key)

      assert {:ok, mutations} = Transaction.mutations(encoded)
      mutations = Enum.to_list(mutations)
      assert {:set, SystemKeys.materializer_key(7), "payload"} in mutations
      assert Enum.any?(mutations, &match?({:set, ^write_key, _fresh}, &1))
    end

    test "supersession is the READ verdict and is authoritative — no commit is attempted" do
      lock = taken_lock()
      usurper = Lock.new_uid()

      deps =
        deps(%{
          get_fn: fn _k, _v -> {:ok, usurper} end,
          commit_fn: fn _p, _e, _t, _o -> flunk("a superseded fence must not commit") end
        })

      assert {:error, :superseded} = Transactions.commit_checked(lock, deps, [])
    end

    test "a commit abort retries with a fresh fence read — a usurper appearing mid-retry is caught by the read" do
      lock = taken_lock()
      usurper = Lock.new_uid()
      counter = :counters.new(1, [])

      deps =
        deps(%{
          get_fn: fn key, _v ->
            if String.ends_with?(key, "owner") do
              # First evaluation: still ours. After the abort: usurped.
              if :counters.get(counter, 1) == 0, do: {:ok, lock.my_owner}, else: {:ok, usurper}
            else
              {:error, :not_found}
            end
          end,
          commit_fn: fn _p, _e, _t, _o ->
            :counters.add(counter, 1, 1)
            {:error, :aborted}
          end
        })

      assert {:error, :superseded} = Transactions.commit_checked(lock, deps, [])
    end

    test "the previous-owner branch reads and conflicts on both keys" do
      test_pid = self()
      prev_owner = Lock.new_uid()
      prev_write = Lock.new_uid()
      {lock, _} = Lock.take(prev_owner, prev_write)

      deps =
        deps(%{
          get_fn: fn key, _v ->
            send(test_pid, {:read, key})
            if String.ends_with?(key, "owner"), do: {:ok, prev_owner}, else: {:ok, prev_write}
          end,
          commit_fn: fn _p, _e, encoded, _o ->
            send(test_pid, {:committed, encoded})
            {:ok, Version.from_integer(5), 0}
          end
        })

      assert :ok = Transactions.commit_checked(lock, deps, [])

      write_key = SystemKeys.distributor_lock_write()
      assert_received {:read, ^write_key}

      assert_received {:committed, encoded}
      assert {:ok, {_v, conflict_ranges}} = Transaction.read_conflicts(encoded)
      assert Enum.any?(conflict_ranges, fn {s, e} -> write_key >= s and write_key < e end)
    end
  end

  describe "commit_checked/3 exhaustion" do
    test "persistent same-owner aborts exhaust into a transient commit failure" do
      {lock, _} = Lock.take(nil, nil)

      deps =
        deps(%{
          get_fn: fn key, _v ->
            if String.ends_with?(key, "owner"), do: {:ok, lock.my_owner}, else: {:error, :not_found}
          end,
          commit_fn: fn _p, _e, _t, _o -> {:error, :aborted} end
        })

      assert {:error, {:lock_commit_failed, :aborted}} = Transactions.commit_checked(lock, deps, [])
    end
  end

  describe "read_snapshot/1" do
    test "reads both families at one pinned version and decodes them" do
      alias Bedrock.SystemKeys.Values, as: V

      test_pid = self()
      v = Version.from_integer(77)

      shard_entries = [{SystemKeys.shard_key(<<0xFF, 0xFF>>), V.encode_shard_key_entry(0, <<>>)}]
      ref_entries = [{SystemKeys.materializer_key(0), V.encode_materializer_ref("wkr", "n@h")}]

      deps =
        deps(%{
          next_read_version_fn: fn -> {:ok, v} end,
          get_range_fn: fn start_key, _end_key, version ->
            send(test_pid, {:range_read, start_key, version})

            cond do
              String.starts_with?(start_key, SystemKeys.shard_keys_prefix()) -> {:ok, {shard_entries, false}}
              String.starts_with?(start_key, SystemKeys.materializers_prefix()) -> {:ok, {ref_entries, false}}
            end
          end
        })

      assert {:ok, %{shard_layout: layout, materializer_refs: refs}} = Transactions.read_snapshot(deps)
      assert layout == %{<<0xFF, 0xFF>> => {0, <<>>}}
      assert refs == %{0 => {"wkr", "n@h"}}

      assert_received {:range_read, _shard_start, ^v}
      assert_received {:range_read, _refs_start, ^v}
    end
  end

  describe "poll_verdict/2" do
    test "reads both keys read-only and mirrors the Lock verdict" do
      {lock, _mutations} = Lock.take(nil, nil)

      assert Transactions.poll_verdict(lock, deps(%{get_fn: fn _k, _v -> {:error, :not_found} end})) == :ok

      usurper = Lock.new_uid()

      assert Transactions.poll_verdict(
               lock,
               deps(%{get_fn: fn _k, _v -> {:ok, usurper} end})
             ) == :superseded
    end

    test "a failed poll read is not a verdict — retry on the next tick" do
      {lock, _mutations} = Lock.take(nil, nil)

      assert Transactions.poll_verdict(lock, deps(%{get_fn: fn _k, _v -> {:failure, :timeout, :ref} end})) ==
               :unavailable
    end
  end
end
