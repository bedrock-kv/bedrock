defmodule Bedrock.DataPlane.Storage.Olivine.State do
  @moduledoc false

  alias Bedrock.ControlPlane.Director
  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Internal.WaitingList
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  @type t :: %__MODULE__{
          otp_name: atom(),
          path: Path.t(),
          foreman: Foreman.ref(),
          id: Worker.id(),
          database: Database.t(),
          index_manager: IndexManager.t(),
          pull_task: Task.t() | nil,
          epoch: Bedrock.epoch() | nil,
          director: Director.ref() | nil,
          mode: :locked | :running,
          waiting_fetches: WaitingList.t(),
          active_tasks: MapSet.t(pid()),
          transaction_queue: :queue.queue(),
          buffer_tracking_queue: :queue.queue()
        }
  defstruct otp_name: nil,
            path: nil,
            foreman: nil,
            id: nil,
            database: nil,
            index_manager: nil,
            pull_task: nil,
            epoch: nil,
            director: nil,
            mode: :locked,
            waiting_fetches: %{},
            active_tasks: MapSet.new(),
            transaction_queue: :queue.new(),
            buffer_tracking_queue: :queue.new()

  @spec update_mode(t(), :locked | :running) :: t()
  def update_mode(t, mode), do: %{t | mode: mode}

  @spec update_director_and_epoch(t(), Director.ref() | nil, Bedrock.epoch() | nil) :: t()
  def update_director_and_epoch(t, director, epoch), do: %{t | director: director, epoch: epoch}

  @spec reset_puller(t()) :: t()
  def reset_puller(t), do: %{t | pull_task: nil}

  @spec put_puller(t(), Task.t()) :: t()
  def put_puller(t, pull_task), do: %{t | pull_task: pull_task}

  @spec add_active_task(t(), pid()) :: t()
  def add_active_task(t, task_pid) do
    Process.monitor(task_pid)
    %{t | active_tasks: MapSet.put(t.active_tasks, task_pid)}
  end

  @spec remove_active_task(t(), pid()) :: t()
  def remove_active_task(t, task_pid), do: %{t | active_tasks: MapSet.delete(t.active_tasks, task_pid)}

  @spec get_active_tasks(t()) :: MapSet.t(pid())
  def get_active_tasks(t), do: t.active_tasks

  @spec queue_transactions(t(), [binary()]) :: t()
  def queue_transactions(t, encoded_transactions) do
    new_queue =
      Enum.reduce(encoded_transactions, t.transaction_queue, fn encoded_tx, queue ->
        version = Transaction.commit_version!(encoded_tx)
        size = byte_size(encoded_tx)
        :queue.in({encoded_tx, version, size}, queue)
      end)

    %{t | transaction_queue: new_queue}
  end

  @spec take_transaction_batch(t(), pos_integer()) :: {[binary()], t()}
  def take_transaction_batch(t, batch_size) do
    {batch, new_queue} = take_batch(t.transaction_queue, batch_size, [])
    {batch, %{t | transaction_queue: new_queue}}
  end

  @spec take_transaction_batch_by_size(t(), pos_integer()) :: {[binary()], Bedrock.version() | nil, t()}
  def take_transaction_batch_by_size(t, max_size_bytes) do
    {batch, last_version, new_queue} = take_batch_by_size(t.transaction_queue, max_size_bytes, [], 0, nil)
    {batch, last_version, %{t | transaction_queue: new_queue}}
  end

  defp take_batch(queue, 0, acc), do: {Enum.reverse(acc), queue}

  defp take_batch(queue, remaining, acc) do
    case :queue.out(queue) do
      {{:value, {transaction, _version, _size}}, new_queue} ->
        take_batch(new_queue, remaining - 1, [transaction | acc])

      {:empty, queue} ->
        {Enum.reverse(acc), queue}
    end
  end

  # Always take at least one transaction, even if it exceeds the size limit
  defp take_batch_by_size(queue, max_size, [_ | _] = acc, current_size, last_version) when current_size >= max_size do
    {Enum.reverse(acc), last_version, queue}
  end

  defp take_batch_by_size(queue, max_size, acc, current_size, last_version) do
    case :queue.out(queue) do
      {{:value, {transaction, version, size}}, new_queue} ->
        new_size = current_size + size
        take_batch_by_size(new_queue, max_size, [transaction | acc], new_size, version)

      {:empty, queue} ->
        {Enum.reverse(acc), last_version, queue}
    end
  end

  @spec queue_empty?(t()) :: boolean()
  def queue_empty?(t), do: :queue.is_empty(t.transaction_queue)

  @spec queue_size(t()) :: non_neg_integer()
  def queue_size(t), do: :queue.len(t.transaction_queue)

  # Buffer tracking queue functions

  @spec add_to_buffer_tracking(t(), Bedrock.version(), pos_integer()) :: t()
  def add_to_buffer_tracking(t, version, size_bytes) do
    new_queue = :queue.in({version, size_bytes}, t.buffer_tracking_queue)
    %{t | buffer_tracking_queue: new_queue}
  end

  @spec get_newest_version_in_buffer(t()) :: Bedrock.version() | nil
  def get_newest_version_in_buffer(t) do
    case :queue.peek_r(t.buffer_tracking_queue) do
      {:value, {version, _size}} -> version
      :empty -> nil
    end
  end

  @spec determine_eviction_batch(t(), pos_integer(), Bedrock.version()) :: {[{Bedrock.version(), pos_integer()}], t()}
  def determine_eviction_batch(t, max_size_bytes, window_edge_version) do
    {batch, new_queue} = take_eviction_batch(t.buffer_tracking_queue, max_size_bytes, window_edge_version, [], 0)
    {batch, %{t | buffer_tracking_queue: new_queue}}
  end

  # Take versions from oldest end of buffer tracking queue until size limit or window edge
  defp take_eviction_batch(queue, max_size, window_edge, acc, current_size) do
    case :queue.peek(queue) do
      {:value, {version, size} = entry} when version <= window_edge and current_size + size < max_size ->
        {_, new_queue} = :queue.out(queue)
        take_eviction_batch(new_queue, max_size, window_edge, [entry | acc], current_size + size)

      # Stop if this version is newer than the window edge (should not be evicted)
      _ ->
        {Enum.reverse(acc), queue}
    end
  end
end
