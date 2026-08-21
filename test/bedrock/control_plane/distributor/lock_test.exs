defmodule Bedrock.ControlPlane.Distributor.LockTest do
  @moduledoc """
  The distributor lock is FDB's MoveKeys lock: ownership enforcement
  lives in the keyspace, not in process supervision. Every mutating
  transaction proves, inside its own serializable commit, that no newer
  owner has appeared — recruitment races and zombie distributors resolve
  by commit conflict, with no consensus beyond the commit pipeline.
  """
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Distributor.Lock
  alias Bedrock.SystemKeys

  describe "take/2" do
    test "on a fresh cluster remembers absent previous state and claims ownership" do
      {lock, mutations} = Lock.take(nil, nil)

      assert lock.prev_owner == nil
      assert lock.prev_write == nil
      assert byte_size(lock.my_owner) == 16

      assert mutations == [{:set, SystemKeys.distributor_lock_owner(), lock.my_owner}]
    end

    test "over an existing owner remembers exactly what it observed" do
      prev_owner = Lock.new_uid()
      prev_write = Lock.new_uid()

      {lock, mutations} = Lock.take(prev_owner, prev_write)

      assert lock.prev_owner == prev_owner
      assert lock.prev_write == prev_write
      assert lock.my_owner != prev_owner
      assert mutations == [{:set, SystemKeys.distributor_lock_owner(), lock.my_owner}]
    end

    test "does NOT touch the write key — FDB parity" do
      # takeMoveKeysLock writes only the Owner key; the Write key is
      # first touched by the checked transaction that follows. Writing it
      # at take time would destroy the interleaved-writer evidence the
      # unobserved-take branch depends on.
      {_lock, mutations} = Lock.take(Lock.new_uid(), Lock.new_uid())

      refute Enum.any?(mutations, fn {:set, key, _} -> key == SystemKeys.distributor_lock_write() end)
    end
  end

  describe "check/3 — the read-check-write fence" do
    test "steady state (owner is mine): touches the write key with a fresh UID" do
      {lock, _} = Lock.take(nil, nil)

      assert {:ok, [{:set, write_key, touch}]} = Lock.check(lock, lock.my_owner, Lock.new_uid())
      assert write_key == SystemKeys.distributor_lock_write()
      assert byte_size(touch) == 16

      # Fresh per check: two touches must differ (FDB's fresh
      # deterministicRandom UID per transaction).
      assert {:ok, [{:set, _, touch2}]} = Lock.check(lock, lock.my_owner, touch)
      assert touch2 != touch
    end

    test "unobserved take (owner still the previous, write untouched): re-asserts ownership" do
      prev_owner = Lock.new_uid()
      prev_write = Lock.new_uid()
      {lock, _} = Lock.take(prev_owner, prev_write)

      assert {:ok, mutations} = Lock.check(lock, prev_owner, prev_write)

      assert {:set, SystemKeys.distributor_lock_owner(), lock.my_owner} in mutations

      assert [{:set, _write_key, fresh_write}] =
               Enum.filter(mutations, fn {:set, key, _} -> key == SystemKeys.distributor_lock_write() end)

      assert byte_size(fresh_write) == 16
      assert fresh_write != prev_write
    end

    test "unobserved take on a fresh cluster (both nil) also re-asserts" do
      {lock, _} = Lock.take(nil, nil)

      assert {:ok, mutations} = Lock.check(lock, nil, nil)
      assert {:set, SystemKeys.distributor_lock_owner(), lock.my_owner} in mutations
    end

    test "an interleaved writer under the previous owner is supersession" do
      # Owner key unchanged, but someone committed a checked transaction
      # (fresh Write UID) under that owner after our take read it: our
      # view of the world is stale and we must not write.
      prev_owner = Lock.new_uid()
      prev_write = Lock.new_uid()
      {lock, _} = Lock.take(prev_owner, prev_write)

      assert {:error, :superseded} = Lock.check(lock, prev_owner, Lock.new_uid())
    end

    test "a new owner is supersession" do
      {lock, _} = Lock.take(nil, nil)

      assert {:error, :superseded} = Lock.check(lock, Lock.new_uid(), Lock.new_uid())
    end
  end

  describe "poll/3 — the read-only poll-to-die verdict" do
    test "mirrors check verdicts without proposing mutations" do
      prev_owner = Lock.new_uid()
      prev_write = Lock.new_uid()
      {lock, _} = Lock.take(prev_owner, prev_write)

      assert Lock.poll(lock, lock.my_owner, Lock.new_uid()) == :ok
      assert Lock.poll(lock, prev_owner, prev_write) == :ok
      assert Lock.poll(lock, prev_owner, Lock.new_uid()) == :superseded
      assert Lock.poll(lock, Lock.new_uid(), Lock.new_uid()) == :superseded
    end
  end

  describe "new_uid/0" do
    test "is 16 bytes and collision-averse" do
      uids = for _ <- 1..64, do: Lock.new_uid()

      assert Enum.all?(uids, &(byte_size(&1) == 16))
      assert length(Enum.uniq(uids)) == 64
    end
  end
end
