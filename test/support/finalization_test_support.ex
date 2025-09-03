defmodule FinalizationTestSupport do
  @moduledoc """
  Shared test utilities and fixtures for finalization tests.
  """

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false

    def name, do: "test_cluster"

    def otp_name(component) when is_atom(component) do
      :"test_cluster_#{component}"
    end
  end

  # Fake sequencer for testing finalization without self-calls
  defmodule FakeSequencer do
    @moduledoc false
    use GenServer

    def start_link(opts \\ []) do
      GenServer.start_link(__MODULE__, :ok, opts)
    end

    def init(:ok), do: {:ok, %{}}

    def handle_call({:report_successful_commit, _commit_version}, _from, state) do
      {:reply, :ok, state}
    end
  end

  # Fake resolver for testing conflict resolution without self-calls
  defmodule FakeResolver do
    @moduledoc false
    use GenServer

    def start_link(opts \\ []) do
      GenServer.start_link(__MODULE__, :ok, opts)
    end

    def init(:ok), do: {:ok, %{}}

    def handle_call(
          {:resolve_transactions, _epoch, {_last_version, _commit_version}, _transaction_summaries},
          _from,
          state
        ) do
      # Return no conflicts (empty list) for simple test scenarios
      {:reply, {:ok, []}, state}
    end
  end

  @doc """
  Creates a fake sequencer that handles synchronous report_successful_commit calls.
  Uses start_supervised! for proper test lifecycle management.
  """
  def create_fake_sequencer do
    ExUnit.Callbacks.start_supervised!(FakeSequencer)
  end

  @doc """
  Creates a fake resolver that handles resolve_transactions calls without conflicts.
  Uses start_supervised! for proper test lifecycle management.
  """
  def create_fake_resolver do
    ExUnit.Callbacks.start_supervised!(FakeResolver)
  end

  @doc """
  Creates a mock log server that responds to GenServer calls.
  Automatically registers cleanup via on_exit to ensure the process is killed.
  """
  def create_mock_log_server do
    pid =
      spawn(fn ->
        receive do
          {:"$gen_call", from, {:push, _transaction, _last_version}} ->
            GenServer.reply(from, :ok)
        after
          5000 -> :timeout
        end
      end)

    ensure_process_killed(pid)
    pid
  end

  @doc """
  Creates a basic transaction system layout for testing.
  """
  def basic_transaction_system_layout(log_server) do
    %{
      sequencer: :test_sequencer,
      resolvers: [{<<0>>, :test_resolver}],
      logs: %{"log_1" => [0, 1]},
      services: %{
        "log_1" => %{kind: :log, status: {:up, log_server}}
      },
      storage_teams: [
        %{tag: 0, key_range: {<<>>, :end}, storage_ids: ["storage_1"]}
      ]
    }
  end

  @doc """
  Creates a multi-log transaction system layout for testing.
  """
  def multi_log_transaction_system_layout do
    %{
      logs: %{
        "log_1" => [0],
        "log_2" => [1],
        "log_3" => [2]
      },
      services: %{
        "log_1" => %{kind: :log, status: {:up, self()}},
        "log_2" => %{kind: :log, status: {:up, self()}},
        "log_3" => %{kind: :log, status: {:up, self()}}
      }
    }
  end

  @doc """
  Creates sample storage teams configuration for testing.
  """
  def sample_storage_teams do
    [
      %{tag: 0, key_range: {<<"a">>, <<"m">>}, storage_ids: ["storage_1"]},
      %{tag: 1, key_range: {<<"m">>, <<"z">>}, storage_ids: ["storage_2"]},
      %{tag: 2, key_range: {<<"z">>, :end}, storage_ids: ["storage_3"]}
    ]
  end

  # Helper function to create test resolver task
  defp create_test_resolver_task(binary) do
    Task.async(fn -> %{:test_resolver => binary} end)
  end

  @doc """
  Creates a test batch with given parameters.
  """
  def create_test_batch(commit_version, last_commit_version, transactions \\ []) do
    # Ensure versions are in proper Bedrock.version() binary format
    commit_version =
      if is_integer(commit_version),
        do: Version.from_integer(commit_version),
        else: commit_version

    last_commit_version =
      if is_integer(last_commit_version),
        do: Version.from_integer(last_commit_version),
        else: last_commit_version

    # Create binary transaction using Transaction encoding
    default_transaction_map = %{
      mutations: [{:set, <<"key1">>, <<"value1">>}],
      write_conflicts: [{<<"key1">>, <<"key1\0">>}],
      read_conflicts: nil
    }

    default_binary = Transaction.encode(default_transaction_map)

    # Create a simple task that returns single resolver map (for tests)
    default_task = create_test_resolver_task(default_binary)

    default_transactions = [
      {0, fn result -> send(self(), {:reply, result}) end, default_binary, default_task}
    ]

    buffer = if Enum.empty?(transactions), do: default_transactions, else: transactions

    # Ensure buffer contains indexed transactions
    indexed_buffer =
      case buffer do
        # If buffer already has indexed format {index, reply_fn, binary, task}, use as-is
        [{_idx, _reply_fn, _binary, _task} | _] ->
          buffer

        # If buffer has old format {reply_fn, binary}, add indices and tasks
        _ ->
          buffer
          |> Enum.with_index()
          |> Enum.map(fn {{reply_fn, binary}, idx} ->
            task = create_test_resolver_task(binary)
            {idx, reply_fn, binary, task}
          end)
      end

    %Bedrock.DataPlane.CommitProxy.Batch{
      commit_version: commit_version,
      last_commit_version: last_commit_version,
      n_transactions: length(indexed_buffer),
      buffer: indexed_buffer
    }
  end

  @doc """
  Creates an all_logs_reached callback for testing.
  """
  def create_all_logs_reached_callback(test_pid \\ nil) do
    target_pid = test_pid || self()

    fn version ->
      send(target_pid, {:all_logs_reached, version})
      :ok
    end
  end

  @doc """
  Ensures a process is killed on test exit.
  """
  def ensure_process_killed(pid) when is_pid(pid) do
    ExUnit.Callbacks.on_exit(fn ->
      if Process.alive?(pid), do: Process.exit(pid, :kill)
    end)
  end

  @doc """
  Creates a mock async stream function that simulates log responses.
  """
  def mock_async_stream_with_responses(responses) do
    fn logs, _fun, _opts ->
      Enum.map(logs, fn {log_id, _service_descriptor} ->
        process_log_response(log_id, responses)
      end)
    end
  end

  defp process_log_response(log_id, responses) do
    case Map.get(responses, log_id) do
      :ok -> {:ok, {log_id, :ok}}
      {:error, reason} -> {:ok, {log_id, {:error, reason}}}
      # Default to success
      nil -> {:ok, {log_id, :ok}}
    end
  end
end
