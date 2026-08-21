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
        commit_fn: fn _proxy, _epoch, _encoded, _opts -> {:ok, v, 0} end
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

    test "a commit abort is supersession — a newer owner won the race" do
      deps = deps(%{commit_fn: fn _proxy, _epoch, _encoded, _opts -> {:error, :aborted} end})

      assert {:error, :superseded} = Transactions.take_lock(deps)
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
