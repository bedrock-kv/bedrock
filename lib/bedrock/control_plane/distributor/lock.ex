defmodule Bedrock.ControlPlane.Distributor.Lock do
  @moduledoc """
  The distributor's write fence: a port of FoundationDB's MoveKeys lock
  (`fdbserver/MoveKeys.actor.cpp`, `takeMoveKeysLock`/
  `checkPersistentMoveKeysLock`).

  Ownership enforcement lives in the keyspace, not in process
  supervision: two system keys (`distributor_lock/owner`,
  `distributor_lock/write`) hold opaque UIDs, and every mutating
  distributor transaction proves — inside its own serializable commit —
  that no newer owner has appeared. Supervision-level singleton-ness
  (the director recruiting one distributor per epoch) is best-effort;
  this lock is what makes a zombie's writes impossible rather than
  merely unlikely.

  The protocol, exactly FDB's:

  - **take** reads both keys, remembers what it observed
    (`prev_owner`/`prev_write`), and writes a fresh `my_owner` UID to
    the owner key — and deliberately NOT the write key: the write key's
    untouched-ness is the evidence the unobserved-take branch depends
    on.
  - **check**, prepended to every mutating transaction, branches three
    ways: owner is mine → touch the write key with a fresh UID (the
    touch is what makes any concurrent take conflict with this commit);
    owner is still the previous one → if the write key also still holds
    what take observed, our take-commit simply hasn't been observed and
    ownership is re-asserted (owner + fresh write), otherwise someone
    else committed under that owner after our read and we are
    superseded; anyone else owns → superseded.
  - **poll** is the read-only verdict for the poll-to-die loop
    (`pollMoveKeysLock`, every ~5s): a superseded distributor exits
    promptly even when idle.

  Named divergence: FDB additionally marks the write-key touch
  self-conflicting so a transaction cannot commit twice
  (`makeSelfConflicting`); Bedrock's commit pipeline never re-submits a
  transaction (fail-fast, no proxy retries), so the hazard that guards
  is structurally precluded here.

  These functions are pure — reads in, mutations out. The transaction
  runner that supplies the reads and commits the mutations arrives with
  the distributor's mutating operations (bedrock-q67.21.4).
  """

  alias Bedrock.SystemKeys

  @type uid :: <<_::128>>
  @type mutation :: {:set, Bedrock.key(), uid()}

  @type t :: %__MODULE__{
          my_owner: uid(),
          prev_owner: uid() | nil,
          prev_write: uid() | nil
        }
  @enforce_keys [:my_owner]
  defstruct [:my_owner, :prev_owner, :prev_write]

  @doc "A fresh opaque 16-byte lock UID."
  @spec new_uid() :: uid()
  def new_uid, do: :crypto.strong_rand_bytes(16)

  @doc """
  Claims ownership: remembers the observed owner/write values and
  proposes writing a fresh owner UID. Run the returned mutations in the
  same transaction as the reads that produced the arguments.
  """
  @spec take(current_owner :: uid() | nil, current_write :: uid() | nil) :: {t(), [mutation()]}
  def take(current_owner, current_write) do
    lock = %__MODULE__{my_owner: new_uid(), prev_owner: current_owner, prev_write: current_write}
    {lock, [{:set, SystemKeys.distributor_lock_owner(), lock.my_owner}]}
  end

  @doc """
  The read-check-write fence: call with the owner/write values read
  inside the mutating transaction; commit the returned mutations with
  it, or abandon the transaction (and the distributor) on
  `{:error, :superseded}`.

  Runner obligations (bedrock-q67.21.4):

  - Read the owner key first; read the write key ONLY when the owner is
    not `my_owner` — the steady-state branch ignores `current_write`
    (pass nil). FDB reads the write key only in the previous-owner
    branch for a reason: an unconditional read puts a read conflict on
    a key this same transaction writes, which would make every pair of
    concurrent same-owner distributor transactions mutually conflict
    and serialize.
  - Pass an absent key through as nil, UNDECODED: nil is
    protocol-meaningful here (fresh cluster / stomped lock), while
    `Values.decode_lock_uid(nil)` is a decode error by design.
  """
  @spec check(t(), current_owner :: uid() | nil, current_write :: uid() | nil) ::
          {:ok, [mutation()]} | {:error, :superseded}
  def check(%__MODULE__{my_owner: mine}, mine, _current_write),
    do: {:ok, [{:set, SystemKeys.distributor_lock_write(), new_uid()}]}

  def check(%__MODULE__{prev_owner: prev, prev_write: prev_write} = lock, prev, current_write) do
    if current_write == prev_write do
      {:ok,
       [
         {:set, SystemKeys.distributor_lock_owner(), lock.my_owner},
         {:set, SystemKeys.distributor_lock_write(), new_uid()}
       ]}
    else
      {:error, :superseded}
    end
  end

  def check(%__MODULE__{}, _current_owner, _current_write), do: {:error, :superseded}

  @doc """
  The read-only poll-to-die verdict: same branches as `check/3`, no
  mutations. A superseded distributor stops rather than waiting to lose
  a commit race.
  """
  @spec poll(t(), current_owner :: uid() | nil, current_write :: uid() | nil) :: :ok | :superseded
  def poll(%__MODULE__{} = lock, current_owner, current_write) do
    case check(lock, current_owner, current_write) do
      {:ok, _mutations} -> :ok
      {:error, :superseded} -> :superseded
    end
  end
end
