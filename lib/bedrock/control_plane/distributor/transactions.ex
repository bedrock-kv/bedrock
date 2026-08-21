defmodule Bedrock.ControlPlane.Distributor.Transactions do
  @moduledoc """
  The distributor's fenced system transactions.

  Every mutating commit is built the FDB way: read the lock state at a
  pinned version, evaluate the `Lock` fence, and commit the fence's
  mutations WITH read conflicts on the lock keys at that version — so a
  concurrent take conflicts with this commit inside the pipeline itself.
  A commit abort is therefore an authoritative supersession verdict (the
  replacement for phase-a's director-side delta rejection); transient
  read/commit failures are surfaced as themselves and are not verdicts.

  Reads resolve through the same channel clients use: a commit proxy's
  covering entry names the system-shard materializer, and the versioned
  read happens there. The keyspace is the channel, for the distributor
  too.
  """

  alias Bedrock.ControlPlane.Distributor.Lock
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.Internal.TransactionBuilder.Tx
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Reader

  @take_attempts 3

  @typedoc """
  The injectable dependency set. `deps_for/4` builds the production
  wiring; tests script the three seams directly.
  """
  @type deps :: %{
          required(:epoch) => Bedrock.epoch(),
          required(:proxies) => [CommitProxy.ref()],
          required(:next_read_version_fn) => (-> {:ok, Bedrock.version()} | {:error, term()}),
          required(:get_fn) => (Bedrock.key(), Bedrock.version() ->
                                  {:ok, binary()} | {:error, :not_found | term()} | {:failure, term(), term()}),
          required(:commit_fn) => (CommitProxy.ref(), Bedrock.epoch(), binary(), keyword() ->
                                     {:ok, Bedrock.version(), non_neg_integer()} | {:error, term()}),
          required(:get_range_fn) => (Bedrock.key(), Bedrock.key(), Bedrock.version() ->
                                        {:ok, {[{Bedrock.key(), binary()}], boolean()}}
                                        | {:error, term()}
                                        | {:failure, term(), term()})
        }

  @doc """
  Production dependencies: read versions from the epoch's sequencer,
  reads resolved by-key through a commit proxy to the covering
  materializer, commits through a random proxy.
  """
  @spec deps_for(module(), Bedrock.epoch(), Sequencer.ref(), [CommitProxy.ref()]) :: deps()
  def deps_for(cluster, epoch, sequencer, proxies) do
    %{
      epoch: epoch,
      proxies: proxies,
      # Every call is bounded: an alive-but-wedged callee must surface as
      # a transient error the director's retry can recruit past — an
      # unbounded call would wedge the distributor invisibly (the
      # director sees a healthy monitored singleton forever).
      next_read_version_fn: fn -> Sequencer.next_read_version(sequencer, epoch, timeout_in_ms: 5_000) end,
      get_fn: fn key, version ->
        case CommitProxy.fetch_routing(Enum.random(proxies), key) do
          {:ok, {_start, _end, _tag, {worker_id, node}}} ->
            # The documented exception to no-atoms-on-decode: system-
            # mode-gated writers, count bounded by cluster membership.
            # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
            Materializer.get({cluster.otp_name_for_worker(worker_id), String.to_atom(node)}, key, version,
              timeout: 5_000
            )

          # Routing's :not_found means UNROUTABLE — a different fact from
          # a key that is readable-and-absent, which only the
          # materializer may assert.
          {:error, :not_found} ->
            {:error, {:unroutable_system_key, key}}

          {:error, reason} ->
            {:error, reason}
        end
      end,
      commit_fn: fn proxy, commit_epoch, encoded, opts ->
        CommitProxy.commit(proxy, commit_epoch, encoded, Keyword.put(opts, :timeout_in_ms, 10_000))
      end,
      get_range_fn: fn start_key, end_key, version ->
        case CommitProxy.fetch_routing(Enum.random(proxies), start_key) do
          {:ok, {_s, _e, _tag, {worker_id, node}}} ->
            # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
            Materializer.get_range(
              {cluster.otp_name_for_worker(worker_id), String.to_atom(node)},
              start_key,
              end_key,
              version,
              limit: 1000,
              timeout: 5_000
            )

          {:error, :not_found} ->
            {:error, {:unroutable_system_key, start_key}}

          {:error, reason} ->
            {:error, reason}
        end
      end
    }
  end

  @doc """
  Reads the distributor's snapshot of the durable mapping families —
  shard layout and materializer refs — at one pinned version (a
  single-version multi-page read cannot tear: the families are written
  transactionally). Read AFTER the lock is taken, under FDB's
  lock-first-snapshot-second startup order.
  """
  @spec read_snapshot(deps()) ::
          {:ok,
           %{
             shard_layout: %{Bedrock.key() => {Bedrock.range_tag(), Bedrock.key()}},
             materializer_refs: %{Bedrock.range_tag() => {String.t(), String.t()}}
           }}
          | {:error, term()}
  def read_snapshot(deps) do
    shard_prefix = SystemKeys.shard_keys_prefix()
    {_s1, shard_end} = Bedrock.KeyRange.from_prefix(shard_prefix)
    refs_prefix = SystemKeys.materializers_prefix()
    {_s2, refs_end} = Bedrock.KeyRange.from_prefix(refs_prefix)

    with {:ok, version} <- read_version(deps),
         {:ok, shard_entries} <-
           Reader.read_family(&deps.get_range_fn.(&1, shard_end, version), shard_prefix, :snapshot_read_failed),
         {:ok, ref_entries} <-
           Reader.read_family(&deps.get_range_fn.(&1, refs_end, version), refs_prefix, :snapshot_read_failed),
         {:ok, shard_layout} <- Reader.shard_layout_from_entries(shard_entries),
         {:ok, refs} <- Reader.decode_materializer_refs(ref_entries) do
      {:ok, %{shard_layout: shard_layout, materializer_refs: refs}}
    end
  end

  @doc """
  A CHECK-fenced mutating commit, honoring the Lock runner obligations:
  the owner key is read (and read-conflicted) always; the write key is
  read only when the owner is not ours — an unconditional write-key
  read would make every pair of concurrent same-owner distributor
  transactions mutually conflict and serialize.

  Verdict semantics mirror FDB exactly: supersession is the READ
  verdict (`Lock.check`'s refusal — FDB's `movekeys_conflict`), and it
  is authoritative. A commit ABORT is not: it retries with a fresh read
  version (re-evaluating the fence, so a genuine usurper is caught by
  the read on the retry); exhausted retries surface as a transient
  commit failure.
  """
  @spec commit_checked(Lock.t(), deps(), [Lock.mutation() | {:set, Bedrock.key(), binary()}]) ::
          :ok
          | {:error, :superseded}
          | {:error, {:read_version_failed | :lock_read_failed | :lock_commit_failed, term()}}
  def commit_checked(lock, deps, payload_mutations), do: commit_checked(lock, deps, payload_mutations, @take_attempts)

  defp commit_checked(lock, deps, payload_mutations, attempts_left) do
    with {:ok, version} <- read_version(deps),
         {:ok, owner} <- read_lock_key(deps, SystemKeys.distributor_lock_owner(), version),
         {:ok, write, write_read?} <- maybe_read_write_key(lock, deps, owner, version),
         {:ok, fence_mutations} <- Lock.check(lock, owner, write) do
      conflict_keys =
        if write_read?,
          do: [SystemKeys.distributor_lock_owner(), SystemKeys.distributor_lock_write()],
          else: [SystemKeys.distributor_lock_owner()]

      case commit_with_conflicts(deps, version, conflict_keys, fence_mutations ++ payload_mutations) do
        :ok ->
          :ok

        {:error, :aborted} when attempts_left > 1 ->
          commit_checked(lock, deps, payload_mutations, attempts_left - 1)

        {:error, reason} ->
          {:error, {:lock_commit_failed, reason}}
      end
    end
  end

  defp maybe_read_write_key(%Lock{my_owner: mine}, _deps, mine, _version), do: {:ok, nil, false}

  defp maybe_read_write_key(_lock, deps, _other_owner, version) do
    with {:ok, write} <- read_lock_key(deps, SystemKeys.distributor_lock_write(), version) do
      {:ok, write, true}
    end
  end

  @doc """
  Takes the distributor lock: reads both lock keys at a pinned version
  (FDB's take reads both — the remembered write UID is the
  unobserved-take evidence), claims the owner key, and commits with read
  conflicts on both keys at that version.

  An abort at TAKE time is not a verdict — FDB's `takeMoveKeysLock`
  retries `not_committed`/`too_old` with a fresh read version, because
  an abort here can also mean the read version fell below the
  resolver's pruning floor. Take semantics are last-take-wins: the
  re-take reads the interleaved winner as the new previous owner and
  claims over it; supersession is delivered where it is authoritative —
  by the CHECK fence on mutating transactions and the poll loop.
  Exhausted retries surface as a transient commit failure for the
  director's recruit-retry.
  """
  @spec take_lock(deps()) ::
          {:ok, Lock.t()}
          | {:error, {:read_version_failed | :lock_read_failed | :lock_commit_failed, term()}}
  def take_lock(deps), do: take_lock(deps, @take_attempts)

  defp take_lock(deps, attempts_left) do
    with {:ok, version} <- read_version(deps),
         {:ok, owner} <- read_lock_key(deps, SystemKeys.distributor_lock_owner(), version),
         {:ok, write} <- read_lock_key(deps, SystemKeys.distributor_lock_write(), version) do
      {lock, mutations} = Lock.take(owner, write)

      case commit_fenced(deps, version, mutations) do
        :ok -> {:ok, lock}
        {:error, :aborted} when attempts_left > 1 -> take_lock(deps, attempts_left - 1)
        {:error, reason} -> {:error, {:lock_commit_failed, reason}}
      end
    end
  end

  @doc """
  The read-only poll-to-die verdict: reads both lock keys at a fresh
  version and mirrors `Lock.poll/3`. A failed read is `:unavailable` —
  not a verdict; the poll loop simply tries again on its next tick.
  """
  @spec poll_verdict(Lock.t(), deps()) :: :ok | :superseded | :unavailable
  def poll_verdict(lock, deps) do
    with {:ok, version} <- read_version(deps),
         {:ok, owner} <- read_lock_key(deps, SystemKeys.distributor_lock_owner(), version),
         {:ok, write} <- read_lock_key(deps, SystemKeys.distributor_lock_write(), version) do
      Lock.poll(lock, owner, write)
    else
      _transient -> :unavailable
    end
  end

  defp read_version(%{next_read_version_fn: next_read_version_fn}) do
    case next_read_version_fn.() do
      {:ok, version} -> {:ok, version}
      {:error, reason} -> {:error, {:read_version_failed, reason}}
    end
  end

  # An absent key is protocol-meaningful nil (fresh cluster / stomped
  # lock) and is passed through UNDECODED — see Lock.check/3's runner
  # obligations.
  defp read_lock_key(%{get_fn: get_fn}, key, version) do
    case get_fn.(key, version) do
      {:ok, value} -> {:ok, value}
      {:error, :not_found} -> {:ok, nil}
      {:error, reason} -> {:error, {:lock_read_failed, reason}}
      {:failure, reason, _ref} -> {:error, {:lock_read_failed, reason}}
    end
  end

  defp commit_fenced(deps, version, mutations) do
    commit_with_conflicts(
      deps,
      version,
      [SystemKeys.distributor_lock_owner(), SystemKeys.distributor_lock_write()],
      mutations
    )
  end

  defp commit_with_conflicts(%{proxies: proxies, epoch: epoch, commit_fn: commit_fn}, version, conflict_keys, mutations) do
    encoded =
      Tx.new()
      |> then(&Enum.reduce(conflict_keys, &1, fn key, tx -> Tx.add_read_conflict_key(tx, key) end))
      |> then(&Enum.reduce(mutations, &1, fn {:set, key, value}, tx -> Tx.set(tx, key, value) end))
      |> Tx.commit(version)

    case commit_fn.(Enum.random(proxies), epoch, encoded, mode: :system) do
      {:ok, _commit_version, _sequence} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
