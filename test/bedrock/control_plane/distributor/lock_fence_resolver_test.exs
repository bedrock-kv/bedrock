defmodule Bedrock.ControlPlane.Distributor.LockFenceResolverTest do
  @moduledoc """
  The owed integration pin (bedrock-q67.21.4): the lock fence's safety
  claim — a concurrent take conflicts inside the commit pipeline — has
  so far been verified against the resolver only by static analysis,
  with commits scripted in unit tests. Here the actual fenced
  transactions the distributor encodes are driven through the REAL
  resolver conflict machinery.
  """
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Distributor.Lock
  alias Bedrock.ControlPlane.Distributor.Transactions
  alias Bedrock.DataPlane.Resolver.ConflictResolution
  alias Bedrock.DataPlane.Resolver.Conflicts
  alias Bedrock.DataPlane.Version
  alias Bedrock.SystemKeys

  # Capture the exact encoded transaction the fenced commit would send.
  defp encoded_fenced(build_fn) do
    test_pid = self()

    deps = %{
      epoch: 1,
      proxies: [:proxy],
      next_read_version_fn: fn -> {:ok, Version.from_integer(10)} end,
      get_fn: fn _key, _version -> {:error, :not_found} end,
      get_range_fn: fn _s, _e, _v -> {:ok, {[], false}} end,
      commit_fn: fn _proxy, _epoch, encoded, _opts ->
        send(test_pid, {:encoded, encoded})
        {:ok, Version.from_integer(11), 0}
      end
    }

    build_fn.(deps)
    assert_received {:encoded, encoded}
    encoded
  end

  test "two concurrent takes at one read version: the resolver aborts the second" do
    take = fn deps -> {:ok, _lock} = Transactions.take_lock(deps) end

    first = encoded_fenced(take)
    second = encoded_fenced(take)

    conflicts = Conflicts.new(Version.zero())

    # Both resolve at the same commit version batch: the first's owner-key
    # write enters history; the second's read conflict on the owner key at
    # the (older) read version must abort — write-after-read-version.
    {conflicts, aborted_first} = ConflictResolution.resolve(conflicts, [first], Version.from_integer(20))
    assert aborted_first == []

    {_conflicts, aborted_second} = ConflictResolution.resolve(conflicts, [second], Version.from_integer(21))
    assert aborted_second == [0]
  end

  test "a check-fenced publish loses to an interleaved take the same way" do
    {lock, _} = Lock.take(nil, nil)

    check = fn deps ->
      # Steady state: the fence reads our own owner UID.
      deps = %{
        deps
        | get_fn: fn key, _v ->
            if String.ends_with?(key, "owner"), do: {:ok, lock.my_owner}, else: {:error, :not_found}
          end
      }

      :ok = Transactions.commit_checked(lock, deps, [{:set, SystemKeys.materializer_key(1), "payload"}])
    end

    take = fn deps -> {:ok, _} = Transactions.take_lock(deps) end

    usurper_take = encoded_fenced(take)
    our_publish = encoded_fenced(check)

    # The usurper's take commits first; our publish — read version pinned
    # before that commit — carries a read conflict on the owner key and
    # must abort. This is the entire mid-epoch safety story: a zombie
    # distributor's writes lose in the pipeline, not by supervision.
    {conflicts, []} =
      ConflictResolution.resolve(Conflicts.new(Version.zero()), [usurper_take], Version.from_integer(20))

    {_conflicts, aborted} = ConflictResolution.resolve(conflicts, [our_publish], Version.from_integer(21))
    assert aborted == [0]
  end

  test "a same-owner publish with no interleaved writer resolves cleanly" do
    {lock, _} = Lock.take(nil, nil)

    check = fn deps ->
      deps = %{
        deps
        | get_fn: fn key, _v ->
            if String.ends_with?(key, "owner"), do: {:ok, lock.my_owner}, else: {:error, :not_found}
          end
      }

      :ok = Transactions.commit_checked(lock, deps, [{:set, SystemKeys.materializer_key(1), "payload"}])
    end

    encoded = encoded_fenced(check)

    {_conflicts, aborted} =
      ConflictResolution.resolve(Conflicts.new(Version.zero()), [encoded], Version.from_integer(20))

    assert aborted == []
  end

  test "sequential same-owner publishes do not conflict with each other — the write key is not read" do
    {lock, _} = Lock.take(nil, nil)

    check = fn deps ->
      deps = %{
        deps
        | get_fn: fn key, _v ->
            if String.ends_with?(key, "owner"), do: {:ok, lock.my_owner}, else: {:error, :not_found}
          end
      }

      :ok = Transactions.commit_checked(lock, deps, [{:set, SystemKeys.materializer_key(1), "payload"}])
    end

    first = encoded_fenced(check)
    second = encoded_fenced(check)

    # If the fence read-conflicted the write key unconditionally, the
    # first publish's write-key touch (committed at 20 > read version 10)
    # would abort the second. The steady-state branch reads only the
    # owner key, so same-owner publishes serialize without conflicting.
    {conflicts, []} = ConflictResolution.resolve(Conflicts.new(Version.zero()), [first], Version.from_integer(20))
    {_conflicts, aborted} = ConflictResolution.resolve(conflicts, [second], Version.from_integer(21))
    assert aborted == []
  end
end
