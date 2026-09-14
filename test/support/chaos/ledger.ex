defmodule Bedrock.Test.Chaos.Ledger do
  @moduledoc """
  The conservation workload: `n` accounts, a fixed starting balance each, and
  transfers between them.

  Money is never created or destroyed, so the sum of all balances is invariant
  at every snapshot version. That single property catches lost writes, torn
  transactions and MVCC violations without any fault injection: a transfer whose
  debit survived but whose credit did not shows up as a total that moved.

  Each transfer also writes a *receipt* in the same transaction, keyed by the
  transfer's id. Receipts turn the invariant from "the total is right" into "the
  total is right *and* it is the total these specific transfers produce", and
  they are what the acked-commit journal is checked against.

  Every function here is a named function in compiled code rather than a closure
  the caller builds. Peers get the primary's code paths, which cover
  `test/support`, but `.exs` files are only ever evaluated in the primary VM, so
  a closure defined in a test file has no module on the peer.
  """

  alias Bedrock.Encoding.Tuple, as: TupleEncoding
  alias Bedrock.Keyspace
  alias Bedrock.Test.Chaos.Repo

  @balances Keyspace.new("chaos/ledger/balance/", key_encoding: TupleEncoding, value_encoding: TupleEncoding)
  @receipts Keyspace.new("chaos/ledger/receipt/", key_encoding: TupleEncoding, value_encoding: TupleEncoding)

  @type account :: non_neg_integer()
  @type transfer_id :: {worker :: non_neg_integer(), seq :: non_neg_integer()}
  @type receipt :: {account(), account(), integer()}
  @type snapshot :: %{balances: %{account() => integer()}, receipts: %{transfer_id() => receipt()}}

  @doc """
  Give every account in `0..account_count-1` the same starting balance.
  """
  @spec seed(pos_integer(), integer()) :: :ok | {:error, term()}
  def seed(account_count, starting_balance) do
    Repo.transact(fn ->
      Enum.each(0..(account_count - 1), fn account -> Repo.put(@balances, account, starting_balance) end)
    end)
  end

  @doc """
  Move `amount` from one account to another, recording a receipt under `id`.

  The receipt is read before anything else so the transfer is idempotent in
  `id`. A commit whose result the client never learns (the commit-unknown-result
  case: the commit lands, the reply is lost) is retried by `Repo.transact`, and
  without that check the retry would apply the transfer a second time against a
  single receipt. That would be indistinguishable from a torn transaction when
  the oracles replay the receipts.
  """
  @spec transfer(transfer_id(), account(), account(), pos_integer()) ::
          :ok | {:error, :already_applied | :insufficient_funds | :no_such_account | term()}
  def transfer(id, from, to, amount) do
    Repo.transact(fn ->
      if Repo.get(@receipts, id) do
        Repo.rollback(:already_applied)
      end

      case {Repo.get(@balances, from), Repo.get(@balances, to)} do
        {nil, _} ->
          Repo.rollback(:no_such_account)

        {_, nil} ->
          Repo.rollback(:no_such_account)

        {from_balance, _to_balance} when from_balance < amount ->
          Repo.rollback(:insufficient_funds)

        {from_balance, to_balance} ->
          Repo.put(@balances, from, from_balance - amount)
          Repo.put(@balances, to, to_balance + amount)
          Repo.put(@receipts, id, {from, to, amount})
          :ok
      end
    end)
  end

  @doc """
  Read every balance and every receipt at one read version.

  Both ranges are read inside a single transaction, so the oracles see one
  consistent snapshot rather than two reads that a concurrent transfer could
  have slipped between. The reads are snapshot reads: this is an observation,
  and making it conflict with the workload would only produce retries.
  """
  @spec snapshot() :: {:ok, snapshot()} | {:error, term()}
  def snapshot do
    Repo.transact(fn ->
      {:ok,
       %{
         balances: @balances |> Repo.get_range(snapshot: true) |> Map.new(),
         receipts: @receipts |> Repo.get_range(snapshot: true) |> Map.new()
       }}
    end)
  end

  @doc """
  Read every receipt at one read version, without the balances.
  """
  @spec receipts() :: {:ok, %{transfer_id() => receipt()}} | {:error, term()}
  def receipts do
    Repo.transact(fn -> {:ok, @receipts |> Repo.get_range(snapshot: true) |> Map.new()} end)
  end
end
