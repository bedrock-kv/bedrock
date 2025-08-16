defmodule Bedrock.DataPlane.Resolver.Server do
  @moduledoc """
  GenServer implementation for the Resolver conflict detection engine.

  Manages resolver state including version ordering through waiting queues.
  Handles out-of-order transaction resolution by queuing later versions until
  earlier ones complete.

  Starts in running mode and is immediately ready to process transaction
  resolution requests.
  """
  alias Bedrock.DataPlane.Resolver.State
  alias Bedrock.DataPlane.Resolver.Tree
  alias Bedrock.DataPlane.Resolver.Validation
  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.WaitingList

  import Bedrock.DataPlane.Resolver.ConflictResolution, only: [resolve: 3]

  use GenServer
  import Bedrock.Internal.GenServer.Replies

  require Logger

  @type reply_fn :: (result :: {:ok, [non_neg_integer()]} | {:error, any()} -> :ok)

  # Default timeout for waiting transactions (30 seconds)
  @default_waiting_timeout_ms 30_000

  @spec child_spec(
          opts :: [
            lock_token: Bedrock.lock_token(),
            key_range: Bedrock.key_range(),
            epoch: Bedrock.epoch()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    lock_token = opts[:lock_token] || raise "Missing :lock_token option"
    _key_range = opts[:key_range] || raise "Missing :key_range option"
    _epoch = opts[:epoch] || raise "Missing :epoch option"

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {lock_token}
         ]},
      restart: :temporary
    }
  end

  @impl true
  def init({lock_token}) do
    zero_version = Version.zero()

    %State{
      lock_token: lock_token,
      tree: %Tree{},
      oldest_version: zero_version,
      last_version: zero_version,
      waiting: %{},
      mode: :running
    }
    |> then(&{:ok, &1})
  end

  @impl true
  def terminate(_reason, _state) do
    :ok
  end

  # When transactions come in order, we can resolve them immediately. Once we're
  # done, we check if there are any transactions waiting for this version to be
  # resolved, and if so, we resolve them as well. We reply to this caller before
  # we do to avoid blocking them.
  @impl true
  def handle_call({:resolve_transactions, {last_version, next_version}, transactions}, _from, t)
      when t.mode == :running and last_version == t.last_version do
    transactions
    |> Validation.check_transactions()
    |> case do
      :ok ->
        {tree, aborted} = resolve(t.tree, transactions, next_version)
        t = %{t | tree: tree, last_version: next_version}

        t |> reply({:ok, aborted}, continue: {:maybe_resolve_next, next_version})

      {:error, reason} ->
        t |> reply({:error, reason}, continue: :check_timeout)
    end
  end

  # When transactions come in a little out of order, we need to wait for the
  # previous transaction to be resolved before we can resolve the next one.
  @impl true
  def handle_call({:resolve_transactions, {last_version, next_version}, transactions}, from, t)
      when t.mode == :running and is_binary(last_version) and last_version > t.last_version do
    transactions
    |> Validation.check_transactions()
    |> case do
      :ok ->
        data = {next_version, transactions}

        {new_waiting, _timeout} =
          WaitingList.insert(
            t.waiting,
            last_version,
            data,
            reply_fn(from),
            @default_waiting_timeout_ms
          )

        %{t | waiting: new_waiting} |> noreply(continue: :check_timeout)

      {:error, reason} ->
        t |> reply({:error, reason}, continue: :check_timeout)
    end
  end

  @impl true
  def handle_info({:resolve_next, next_version}, t) do
    {new_waiting, entry} = WaitingList.remove(t.waiting, next_version)

    t = %{t | waiting: new_waiting}

    case entry do
      {_deadline, reply_fn, {_next_version, transactions}} ->
        {tree, aborted} = resolve(t.tree, transactions, next_version)
        t = %{t | tree: tree, last_version: next_version}

        reply_fn.({:ok, aborted})

        t |> noreply(continue: {:maybe_resolve_next, next_version})

      nil ->
        # Entry not found - shouldn't happen but handle gracefully
        t |> noreply(continue: :check_timeout)
    end
  end

  @impl true
  def handle_info(:timeout, t) do
    {new_waiting, expired_entries} = WaitingList.expire(t.waiting)

    expired_entries
    |> Enum.each(fn {_deadline, reply_fn, _data} ->
      reply_fn.({:error, :waiting_timeout})
    end)

    %{t | waiting: new_waiting} |> noreply(continue: :check_timeout)
  end

  # Check if there are waiting transactions for this version and resolve or set timeout
  @impl true
  def handle_continue({:maybe_resolve_next, next_version}, t) do
    case WaitingList.find(t.waiting, next_version) do
      nil ->
        t |> noreply(continue: :check_timeout)

      _entry ->
        t |> noreply(continue: {:resolve_next, next_version})
    end
  end

  @impl true
  def handle_continue(:check_timeout, t) do
    timeout = WaitingList.next_timeout(t.waiting)
    t |> noreply(timeout: timeout)
  end

  @spec reply_fn(GenServer.from()) :: reply_fn()
  defp reply_fn(from), do: &GenServer.reply(from, &1)
end
