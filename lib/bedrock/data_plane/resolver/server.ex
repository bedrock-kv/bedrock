defmodule Bedrock.DataPlane.Resolver.Server do
  @moduledoc """
  GenServer implementation for the Resolver conflict detection engine.

  Manages resolver state including version ordering through waiting queues.
  Handles out-of-order transaction resolution by queuing later versions until
  earlier ones complete.

  Starts in running mode and is immediately ready to process transaction
  resolution requests.
  """
  use GenServer

  import Bedrock.DataPlane.Resolver.ConflictResolution, only: [resolve: 3]

  import Bedrock.DataPlane.Resolver.Telemetry,
    only: [
      emit_received: 4,
      emit_processing: 3,
      emit_completed: 5,
      emit_reply_sent: 4,
      emit_waiting_list: 4,
      emit_waiting_list_inserted: 5,
      emit_waiting_resolved: 4,
      emit_validation_error: 2,
      emit_waiting_list_validation_error: 2
    ]

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.DataPlane.Resolver.State
  alias Bedrock.DataPlane.Resolver.Tree
  alias Bedrock.DataPlane.Resolver.Validation
  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.WaitingList

  @type reply_fn :: (result :: {:ok, [non_neg_integer()]} | {:error, any()} -> :ok)

  @default_waiting_timeout_ms 30_000

  @spec child_spec(
          opts :: [
            lock_token: Bedrock.lock_token(),
            key_range: Bedrock.key_range(),
            epoch: Bedrock.epoch(),
            last_version: Bedrock.version()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    lock_token = opts[:lock_token] || raise "Missing :lock_token option"
    _key_range = opts[:key_range] || raise "Missing :key_range option"
    epoch = opts[:epoch] || raise "Missing :epoch option"
    last_version = opts[:last_version] || Version.zero()

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {lock_token, last_version, epoch}
         ]},
      restart: :temporary
    }
  end

  @impl true
  def init({lock_token, last_version, epoch}) do
    then(
      %State{
        lock_token: lock_token,
        tree: %Tree{},
        oldest_version: last_version,
        last_version: last_version,
        waiting: %{},
        mode: :running,
        epoch: epoch
      },
      &{:ok, &1}
    )
  end

  @impl true
  def terminate(_reason, _state) do
    :ok
  end

  @impl true
  def handle_call({:resolve_transactions, epoch, {_last_version, _next_version}, _transactions}, _from, t)
      when epoch != t.epoch do
    reply(t, {:error, {:epoch_mismatch, expected: t.epoch, received: epoch}})
  end

  @impl true
  def handle_call({:resolve_transactions, epoch, {last_version, next_version}, transactions}, from, t)
      when t.mode == :running and epoch == t.epoch and last_version == t.last_version do
    emit_received(length(transactions), last_version, next_version, t.last_version)

    transactions
    |> Validation.check_transactions()
    |> case do
      :ok ->
        noreply(t, continue: {:process_ready, {next_version, transactions, reply_fn(from)}})

      {:error, reason} ->
        emit_validation_error(length(transactions), reason)
        reply(t, {:error, reason}, continue: :next_timeout)
    end
  end

  @impl true
  def handle_call({:resolve_transactions, epoch, {last_version, next_version}, transactions}, from, t)
      when t.mode == :running and epoch == t.epoch and is_binary(last_version) and last_version > t.last_version do
    emit_waiting_list(length(transactions), last_version, next_version, t.last_version)

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

        emit_waiting_list_inserted(
          length(transactions),
          map_size(new_waiting),
          last_version,
          next_version,
          t.last_version
        )

        noreply(%{t | waiting: new_waiting}, continue: :next_timeout)

      {:error, reason} ->
        emit_waiting_list_validation_error(length(transactions), reason)
        reply(t, {:error, reason}, continue: :next_timeout)
    end
  end

  @impl true
  def handle_call({:resolve_transactions, epoch, {last_version, _next_version}, transactions}, _from, t)
      when t.mode == :running and epoch == t.epoch and is_binary(last_version) and last_version < t.last_version do
    aborted_indices = Enum.to_list(0..(length(transactions) - 1))
    reply(t, {:ok, aborted_indices})
  end

  @impl true
  def handle_info(:timeout, t) do
    {new_waiting, expired_entries} = WaitingList.expire(t.waiting)

    Enum.each(expired_entries, fn {_deadline, reply_fn, _data} ->
      reply_fn.({:error, :waiting_timeout})
    end)

    noreply(%{t | waiting: new_waiting}, continue: :next_timeout)
  end

  @impl true
  def handle_continue({:process_ready, {next_version, transactions, reply_fn}}, t) do
    emit_processing(length(transactions), t.last_version, next_version)

    {new_waiting, _} = WaitingList.remove(t.waiting, t.last_version)

    {tree, aborted} = resolve(t.tree, transactions, next_version)
    t = %{t | tree: tree, last_version: next_version, waiting: new_waiting}

    emit_completed(
      length(transactions),
      length(aborted),
      t.last_version,
      next_version,
      t.last_version
    )

    reply_fn.({:ok, aborted})

    emit_reply_sent(length(transactions), length(aborted), t.last_version, next_version)

    case WaitingList.find(t.waiting, next_version) do
      nil ->
        noreply(t, continue: :next_timeout)

      {_deadline, reply_fn, {next_version, transactions}} ->
        emit_waiting_resolved(length(transactions), 0, next_version, t.last_version)

        noreply(t, continue: {:process_ready, {next_version, transactions, reply_fn}})
    end
  end

  @impl true
  def handle_continue(:next_timeout, t) do
    timeout = WaitingList.next_timeout(t.waiting)
    noreply(t, timeout: timeout)
  end

  @spec reply_fn(GenServer.from()) :: reply_fn()
  defp reply_fn(from), do: &GenServer.reply(from, &1)
end
