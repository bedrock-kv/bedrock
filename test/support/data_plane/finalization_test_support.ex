defmodule Bedrock.Test.DataPlane.FinalizationTestSupport do
  @moduledoc """
  Shared test utilities and fixtures for finalization tests.
  """

  alias Bedrock.DataPlane.CommitProxy.RoutingData
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

  @doc """
  Creates a fake sequencer that handles synchronous report_successful_commit calls.
  Uses start_supervised! for proper test lifecycle management.
  """
  def create_fake_sequencer do
    ExUnit.Callbacks.start_supervised!(FakeSequencer)
  end

  @doc """
  Creates a mock log server that responds to GenServer calls.
  Automatically registers cleanup via on_exit to ensure the process is killed.
  """
  def create_mock_log_server do
    pid =
      spawn(fn ->
        receive do
          {:"$gen_call", from, {:push, _transaction, _last_version, _kcv}} ->
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
      }
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
  Builds routing data from a transaction system layout.
  Used to seed metadata_apply_fn/1 for finalize_batch opts.
  """
  def build_routing_data(transaction_system_layout) do
    logs = Map.get(transaction_system_layout, :logs, %{})
    services = Map.get(transaction_system_layout, :services, %{})
    shard_layout = Map.get(transaction_system_layout, :shard_layout, default_shard_layout())

    log_map =
      logs
      |> Map.keys()
      |> Enum.sort()
      |> Enum.with_index()
      |> Map.new(fn {log_id, index} -> {index, log_id} end)

    # Build log_services from services map - extract log refs
    log_services =
      logs
      |> Map.keys()
      |> Enum.reduce(%{}, fn log_id, acc ->
        case Map.get(services, log_id) do
          %{kind: :log, status: {:up, pid}} when is_pid(pid) ->
            # For test purposes, store pid as service ref
            Map.put(acc, log_id, pid)

          %{kind: :log, status: {:up, {name, node}}} ->
            Map.put(acc, log_id, {name, node})

          _ ->
            acc
        end
      end)

    replication_factor = max(1, map_size(logs))

    RoutingData.from_snapshot(%{
      shard_layout: shard_layout,
      log_map: log_map,
      log_services: log_services,
      replication_factor: replication_factor
    })
  end

  @doc """
  An exact metadata window with no entries that tiles correctly for a proxy
  whose batches chain from Version.zero(): the first window's from is nil
  (first contact), every later window's from is the batch's last_version -
  which equals the proxy's applied version, exactly as the real resolver
  serves them.
  """
  def tiling_window(last_version, commit_version) do
    from = if last_version == Version.zero(), do: nil, else: last_version
    {from, commit_version, []}
  end

  @doc """
  A stand-in for the commit proxy server's serialized apply-and-route step:
  applies the batch's committed window entries to the given routing data and
  returns the snapshot the batch should push with. The window arrives with
  verdicts already resolved (plain `{version, [mutation]}` entries).
  """
  def metadata_apply_fn(%RoutingData{} = routing_data) do
    fn _commit_version, window ->
      entries =
        case window do
          nil -> []
          {_from, _to, entries} -> entries
        end

      {:ok, RoutingData.apply_mutations(routing_data, entries)}
    end
  end

  # Default shard layout covering entire keyspace with a single shard (tag 0)
  defp default_shard_layout do
    %{<<0xFF, 0xFF>> => {0, <<>>}}
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

    default_transactions = [
      {0, fn result -> send(self(), {:reply, result}) end, default_binary, :user}
    ]

    buffer = if Enum.empty?(transactions), do: default_transactions, else: transactions

    # Ensure buffer contains indexed transactions
    indexed_buffer =
      case buffer do
        # If buffer already has indexed format {index, reply_fn, binary, commit_mode}, use as-is
        [{_idx, _reply_fn, _binary, _commit_mode} | _] ->
          buffer

        # If buffer has {reply_fn, binary} or {reply_fn, binary, mode} entries,
        # add indices (mode defaults to :user)
        _ ->
          buffer
          |> Enum.with_index()
          |> Enum.map(fn
            {{reply_fn, binary}, idx} -> {idx, reply_fn, binary, :user}
            {{reply_fn, binary, mode}, idx} -> {idx, reply_fn, binary, mode}
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
