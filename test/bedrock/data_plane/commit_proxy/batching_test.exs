defmodule Bedrock.DataPlane.CommitProxy.BatchingTest do
  @moduledoc """
  Unit tests for the Batching module.

  Tests cover batch lifecycle management including:
  - Single transaction batch creation
  - Batch start/creation logic
  - Adding transactions to existing batches
  - Finalization policy (size and time-based triggers)
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.Batching
  alias Bedrock.DataPlane.CommitProxy.State
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  # Mock sequencer that returns predictable versions
  # Requires epoch to be passed (validates epoch validation is wired up)
  defmodule MockSequencer do
    @moduledoc false
    use GenServer

    def start_link(opts \\ []) do
      initial_version = Keyword.get(opts, :initial_version, 0)
      expected_epoch = Keyword.get(opts, :expected_epoch, 1)
      GenServer.start_link(__MODULE__, {initial_version, expected_epoch})
    end

    def init(state), do: {:ok, state}

    # Accept calls with matching epoch
    def handle_call({:next_commit_version, epoch}, _from, {counter, epoch}) do
      last_version = Version.from_integer(counter)
      next_version = Version.from_integer(counter + 1)
      {:reply, {:ok, last_version, next_version}, {counter + 1, epoch}}
    end

    # Reject calls with wrong epoch
    def handle_call({:next_commit_version, _wrong_epoch}, _from, state) do
      {:reply, {:error, :wrong_epoch}, state}
    end

    # Reject calls without epoch (legacy format)
    def handle_call(:next_commit_version, _from, state) do
      {:reply, {:error, :epoch_required}, state}
    end
  end

  # Mock sequencer that always returns unavailable
  defmodule UnavailableSequencer do
    @moduledoc false
    use GenServer

    def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, :ok)
    def init(:ok), do: {:ok, :ok}

    def handle_call({:next_commit_version, _epoch}, _from, state) do
      {:reply, {:error, :unavailable}, state}
    end
  end

  defp build_state(overrides) do
    base = %State{
      cluster: nil,
      director: self(),
      epoch: 1,
      max_latency_in_ms: 100,
      max_per_batch: 5,
      empty_transaction_timeout_ms: 1000,
      sequencer: nil,
      batch: nil
    }

    Map.merge(base, overrides)
  end

  defp sample_transaction do
    Transaction.encode(%{
      mutations: [{:set, "key", "value"}],
      read_conflicts: {nil, []},
      write_conflicts: []
    })
  end

  describe "single_transaction_batch/3" do
    test "creates batch when sequencer is available" do
      sequencer = start_supervised!({MockSequencer, initial_version: 100})

      state =
        build_state(%{
          sequencer: sequencer
        })

      transaction = sample_transaction()
      reply_fn = fn _result -> :ok end

      assert {:ok, %Batch{} = batch} = Batching.single_transaction_batch(state, transaction, reply_fn)
      assert batch.n_transactions == 1
      assert batch.last_commit_version == Version.from_integer(100)
      assert batch.commit_version == Version.from_integer(101)
      assert batch.finalized_at
    end

    test "returns error when sequencer is nil" do
      state =
        build_state(%{
          sequencer: nil
        })

      transaction = sample_transaction()

      assert {:error, :sequencer_unavailable} = Batching.single_transaction_batch(state, transaction)
    end

    test "returns error when sequencer returns unavailable" do
      sequencer = start_supervised!(UnavailableSequencer)

      state =
        build_state(%{
          sequencer: sequencer
        })

      transaction = sample_transaction()

      assert {:error, :sequencer_unavailable} = Batching.single_transaction_batch(state, transaction)
    end

    test "uses default reply_fn when not provided" do
      sequencer = start_supervised!({MockSequencer, initial_version: 0})

      state =
        build_state(%{
          sequencer: sequencer
        })

      transaction = sample_transaction()

      # Should not raise when using default reply_fn
      assert {:ok, %Batch{}} = Batching.single_transaction_batch(state, transaction)
    end
  end

  describe "start_batch_if_needed/1" do
    test "creates new batch when batch is nil" do
      sequencer = start_supervised!({MockSequencer, initial_version: 50})

      state =
        build_state(%{
          batch: nil,
          sequencer: sequencer
        })

      result = Batching.start_batch_if_needed(state)

      assert %State{batch: %Batch{} = batch} = result
      assert batch.n_transactions == 0
      assert batch.last_commit_version == Version.from_integer(50)
      assert batch.commit_version == Version.from_integer(51)
    end

    test "returns state unchanged when batch exists" do
      existing_batch = %Batch{
        started_at: 1000,
        last_commit_version: Version.from_integer(10),
        commit_version: Version.from_integer(11),
        n_transactions: 2,
        buffer: []
      }

      state = build_state(%{batch: existing_batch})

      result = Batching.start_batch_if_needed(state)

      assert result == state
      assert result.batch == existing_batch
    end

    test "returns error when sequencer is unavailable" do
      sequencer = start_supervised!(UnavailableSequencer)

      state =
        build_state(%{
          batch: nil,
          sequencer: sequencer
        })

      assert {:error, {:sequencer_unavailable, :unavailable}} = Batching.start_batch_if_needed(state)
    end
  end

  describe "add_transaction_to_batch/4" do
    test "adds transaction to existing batch" do
      existing_batch = %Batch{
        started_at: 1000,
        last_commit_version: Version.from_integer(10),
        commit_version: Version.from_integer(11),
        n_transactions: 0,
        buffer: []
      }

      state = build_state(%{batch: existing_batch})

      transaction = sample_transaction()
      reply_fn = fn _result -> :ok end

      result = Batching.add_transaction_to_batch(state, transaction, reply_fn, nil)

      assert %State{batch: %Batch{} = batch} = result
      assert batch.n_transactions == 1
      assert length(batch.buffer) == 1

      [{index, stored_reply_fn, stored_tx, stored_task}] = batch.buffer
      assert index == 0
      assert stored_reply_fn == reply_fn
      assert stored_tx == transaction
      assert stored_task == nil
    end

    test "increments index for each transaction added" do
      existing_batch = %Batch{
        started_at: 1000,
        last_commit_version: Version.from_integer(10),
        commit_version: Version.from_integer(11),
        n_transactions: 0,
        buffer: []
      }

      state = build_state(%{batch: existing_batch})

      tx1 = sample_transaction()
      tx2 = sample_transaction()
      reply_fn = fn _result -> :ok end

      state = Batching.add_transaction_to_batch(state, tx1, reply_fn, nil)
      state = Batching.add_transaction_to_batch(state, tx2, reply_fn, nil)

      assert state.batch.n_transactions == 2

      # Buffer is in reverse order (newest first)
      [{idx2, _, _, _}, {idx1, _, _, _}] = state.batch.buffer
      assert idx1 == 0
      assert idx2 == 1
    end

    test "stores task reference when provided" do
      existing_batch = %Batch{
        started_at: 1000,
        last_commit_version: Version.from_integer(10),
        commit_version: Version.from_integer(11),
        n_transactions: 0,
        buffer: []
      }

      state = build_state(%{batch: existing_batch})

      transaction = sample_transaction()
      reply_fn = fn _result -> :ok end
      task = Task.async(fn -> :some_result end)

      result = Batching.add_transaction_to_batch(state, transaction, reply_fn, task)

      [{_index, _reply_fn, _tx, stored_task}] = result.batch.buffer
      assert stored_task == task

      # Clean up task
      Task.await(task)
    end
  end

  describe "apply_finalization_policy/1" do
    test "triggers finalization when max_transactions reached" do
      # Create batch at max capacity
      transactions =
        for i <- 0..4 do
          {i, fn _result -> :ok end, sample_transaction(), nil}
        end

      batch = %Batch{
        started_at: :erlang.monotonic_time(:millisecond),
        last_commit_version: Version.from_integer(10),
        commit_version: Version.from_integer(11),
        n_transactions: 5,
        buffer: transactions
      }

      state = build_state(%{batch: batch, max_per_batch: 5})

      {updated_state, finalized_batch} = Batching.apply_finalization_policy(state)

      assert updated_state.batch == nil
      assert finalized_batch
      assert finalized_batch.n_transactions == 5
      assert finalized_batch.finalized_at
    end

    test "triggers finalization when max_latency exceeded" do
      # Create batch that started long ago
      old_started_at = :erlang.monotonic_time(:millisecond) - 200

      batch = %Batch{
        started_at: old_started_at,
        last_commit_version: Version.from_integer(10),
        commit_version: Version.from_integer(11),
        n_transactions: 1,
        buffer: [{0, fn _result -> :ok end, sample_transaction(), nil}]
      }

      state = build_state(%{batch: batch, max_latency_in_ms: 100})

      {updated_state, finalized_batch} = Batching.apply_finalization_policy(state)

      assert updated_state.batch == nil
      assert finalized_batch
      assert finalized_batch.finalized_at
    end

    test "does not trigger finalization when neither policy is met" do
      # Create recent batch with few transactions
      batch = %Batch{
        started_at: :erlang.monotonic_time(:millisecond),
        last_commit_version: Version.from_integer(10),
        commit_version: Version.from_integer(11),
        n_transactions: 1,
        buffer: [{0, fn _result -> :ok end, sample_transaction(), nil}]
      }

      state = build_state(%{batch: batch, max_per_batch: 10, max_latency_in_ms: 10_000})

      {updated_state, finalized_batch} = Batching.apply_finalization_policy(state)

      assert updated_state.batch == batch
      assert finalized_batch == nil
    end

    test "prefers transaction count over latency when both met" do
      # Both conditions met - should still finalize
      old_started_at = :erlang.monotonic_time(:millisecond) - 200

      transactions =
        for i <- 0..4 do
          {i, fn _result -> :ok end, sample_transaction(), nil}
        end

      batch = %Batch{
        started_at: old_started_at,
        last_commit_version: Version.from_integer(10),
        commit_version: Version.from_integer(11),
        n_transactions: 5,
        buffer: transactions
      }

      state = build_state(%{batch: batch, max_per_batch: 5, max_latency_in_ms: 100})

      {updated_state, finalized_batch} = Batching.apply_finalization_policy(state)

      assert updated_state.batch == nil
      assert finalized_batch
    end
  end

  describe "integration: batch lifecycle" do
    test "full lifecycle from creation to finalization" do
      sequencer = start_supervised!({MockSequencer, initial_version: 0})

      state =
        build_state(%{
          batch: nil,
          max_per_batch: 3,
          max_latency_in_ms: 10_000,
          sequencer: sequencer
        })

      # Start a new batch
      state = Batching.start_batch_if_needed(state)
      assert state.batch
      assert state.batch.n_transactions == 0

      # Add transactions
      tx = sample_transaction()
      reply_fn = fn _result -> :ok end

      state = Batching.add_transaction_to_batch(state, tx, reply_fn, nil)
      assert state.batch.n_transactions == 1

      state = Batching.add_transaction_to_batch(state, tx, reply_fn, nil)
      assert state.batch.n_transactions == 2

      # Policy check - not yet ready
      {state, batch_to_finalize} = Batching.apply_finalization_policy(state)
      assert batch_to_finalize == nil
      assert state.batch

      # Add third transaction - should trigger policy
      state = Batching.add_transaction_to_batch(state, tx, reply_fn, nil)
      assert state.batch.n_transactions == 3

      {state, batch_to_finalize} = Batching.apply_finalization_policy(state)
      assert batch_to_finalize
      assert batch_to_finalize.n_transactions == 3
      assert state.batch == nil
    end
  end
end
