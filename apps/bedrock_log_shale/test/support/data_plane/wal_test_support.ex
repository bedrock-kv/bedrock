defmodule Bedrock.Test.DataPlane.WALTestSupport do
  @moduledoc """
  Test support utilities specifically for WAL (Write-Ahead Log) testing.

  This module provides comprehensive helpers for testing the full WAL workflow:
  write transactions to files, read them back by version, and verify consistency.
  Created to catch the :not_found bug where logs claim version ranges they can't retrieve.
  """

  import ExUnit.Callbacks, only: [start_supervised!: 1, on_exit: 1]

  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.Server, as: ShaleServer
  alias Bedrock.DataPlane.Log.Shale.TransactionStreams
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  @doc """
  Creates a test WAL file with multiple transactions at specified versions.

  Returns the file path and a map of version -> transaction_data for verification.

  ## Example

      {file_path, version_map} = create_test_wal([
        {100, %{"account" => "1", "balance" => "50"}},
        {200, %{"account" => "2", "balance" => "75"}},
        {300, %{"account" => "1", "balance" => "25"}}
      ])
  """
  @spec create_test_wal([{integer(), map()}]) :: {String.t(), %{Version.t() => map()}}
  def create_test_wal(version_data_pairs) do
    # Use tmp directory to avoid working directory issues
    temp_dir = System.tmp_dir!()
    # Ensure temp directory exists
    File.mkdir_p!(temp_dir)
    # Make filename more unique to avoid conflicts in parallel tests
    unique_id = "#{System.unique_integer([:positive])}_#{:erlang.monotonic_time()}"
    file_path = Path.join(temp_dir, "test_wal_#{unique_id}.log")

    # Register cleanup for this specific file
    on_exit(fn -> File.rm(file_path) end)

    File.write!(file_path, "")
    {:ok, fd} = File.open(file_path, [:write, :raw, :binary])
    :ok = :file.truncate(fd)
    :ok = :file.write(fd, :binary.copy(<<0>>, 100_000))
    :ok = File.close(fd)

    {:ok, writer} = Writer.open(file_path)

    {final_writer, version_map} =
      Enum.reduce(version_data_pairs, {writer, %{}}, fn {version_int, data}, {acc_writer, acc_map} ->
        version = Version.from_integer(version_int)
        encoded_transaction = TransactionTestSupport.new_log_transaction(version, data)
        {:ok, updated_writer} = Writer.append(acc_writer, encoded_transaction, version)
        updated_map = Map.put(acc_map, version, data)

        {updated_writer, updated_map}
      end)

    Writer.close(final_writer)

    {file_path, version_map}
  end

  @doc """
  Reads a transaction from a WAL file by specific version using TransactionStreams.

  Returns {:ok, transaction_binary} or {:error, reason}.
  This tests the exact code path that's failing in production.
  """
  @spec read_transaction_by_version(String.t(), Version.t()) ::
          {:ok, Transaction.encoded()} | {:error, :not_found} | {:error, term()}
  def read_transaction_by_version(file_path, target_version) do
    segment = %Segment{
      path: file_path,
      min_version: Version.from_integer(0),
      transactions: nil
    }

    with {:ok, stream} <- TransactionStreams.from_segments([segment], target_version),
         [transaction] <- Enum.take(stream, 1),
         {:ok, ^target_version} <- Transaction.commit_version(transaction) do
      {:ok, transaction}
    else
      {:ok, different_version} -> {:error, {:wrong_version, different_version}}
      [] -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
      error -> error
    end
  rescue
    error -> {:error, {:exception, error}}
  end

  @doc """
  Verifies that a transaction contains the expected data.

  Decodes the transaction and checks that all expected key-value pairs are present.
  """
  @spec verify_transaction_content(Transaction.encoded(), map()) :: :ok | {:error, term()}
  def verify_transaction_content(encoded_transaction, expected_data) do
    with {:ok, _decoded} <- Transaction.decode(encoded_transaction),
         {:ok, mutations_stream} <- Transaction.mutations(encoded_transaction) do
      mutations_list = Enum.to_list(mutations_stream)

      expected_mutations = Enum.map(expected_data, fn {key, value} -> {:set, key, value} end)
      missing_mutations = expected_mutations -- mutations_list
      extra_mutations = mutations_list -- expected_mutations

      cond do
        missing_mutations != [] -> {:error, {:missing_mutations, missing_mutations}}
        extra_mutations != [] -> {:error, {:extra_mutations, extra_mutations}}
        true -> :ok
      end
    end
  end

  @doc """
  Creates a comprehensive test scenario with multiple transactions and gaps.

  Returns {file_path, test_scenarios} where test_scenarios contains:
  - :existing_versions - versions that should be found
  - :missing_versions - versions that should return :not_found
  - :boundary_versions - first and last versions
  """
  @spec create_comprehensive_test_scenario() :: {String.t(), map()}
  def create_comprehensive_test_scenario do
    version_data_pairs = [
      {100, %{"step" => "1", "account" => "alice", "balance" => "1000"}},
      {150, %{"step" => "2", "account" => "bob", "balance" => "500"}},
      # Gap from 151-299
      {300, %{"step" => "3", "account" => "alice", "balance" => "800"}},
      {301, %{"step" => "4", "account" => "charlie", "balance" => "200"}},
      # Gap from 302-499
      {500, %{"step" => "5", "account" => "bob", "balance" => "700"}}
    ]

    {file_path, version_map} = create_test_wal(version_data_pairs)

    scenarios = %{
      existing_versions: Map.keys(version_map),
      missing_versions: [
        # Before first
        Version.from_integer(99),
        # In first gap
        Version.from_integer(125),
        # Just before third transaction
        Version.from_integer(299),
        # In second gap
        Version.from_integer(302),
        # After last
        Version.from_integer(501)
      ],
      boundary_versions: %{
        first: Version.from_integer(100),
        last: Version.from_integer(500)
      },
      version_map: version_map
    }

    {file_path, scenarios}
  end

  @doc """
  Runs a comprehensive test of reading all versions from a WAL file.

  This is the test that would have caught the :not_found bug.
  """
  @spec test_all_claimed_versions_are_readable(String.t(), [Version.t()]) ::
          :ok | {:error, {Version.t(), term()}}
  def test_all_claimed_versions_are_readable(file_path, claimed_versions) do
    Enum.reduce_while(claimed_versions, :ok, fn version, :ok ->
      case read_transaction_by_version(file_path, version) do
        {:ok, _transaction} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {version, reason}}}
      end
    end)
  end

  @doc """
  Cleanup helper for test files.
  """
  @spec cleanup_test_file(String.t()) :: :ok
  def cleanup_test_file(file_path) do
    File.rm(file_path)
    :ok
  end

  @doc """
  Creates a transaction for testing with specific version and key-value pairs.
  """
  @spec create_test_transaction(integer(), [{String.t(), String.t()}]) :: Transaction.encoded()
  def create_test_transaction(version_int, key_value_pairs) do
    version = Version.from_integer(version_int)
    mutations = Enum.map(key_value_pairs, fn {k, v} -> {:set, k, v} end)

    encoded = Transaction.encode(%{mutations: mutations})
    {:ok, with_version} = Transaction.add_commit_version(encoded, version)
    with_version
  end

  @doc """
  Extracts the version from an encoded transaction.
  """
  @spec extract_version(Transaction.encoded()) :: Version.t()
  def extract_version(encoded_transaction) do
    {:ok, version} = Transaction.commit_version(encoded_transaction)
    version
  end

  @doc """
  Creates a test log server for integration testing.
  """
  @spec create_test_log() :: {:ok, pid()}
  def create_test_log do
    # Use system temp directory with unique subdirectory
    temp_base = System.tmp_dir!()
    unique_id = "#{System.unique_integer([:positive])}_#{:erlang.monotonic_time()}"
    test_dir = Path.join(temp_base, "wal_test_#{unique_id}")
    File.mkdir_p!(test_dir)

    # Register cleanup for this specific directory
    on_exit(fn -> File.rm_rf(test_dir) end)

    cluster = Bedrock.Cluster
    otp_name = :"test_log_#{System.unique_integer([:positive])}"
    id = "test_log_#{System.unique_integer([:positive])}"

    log_opts = [
      cluster: cluster,
      otp_name: otp_name,
      id: id,
      foreman: self(),
      path: test_dir,
      # Start in :running mode
      start_unlocked: true
    ]

    pid = start_supervised!(ShaleServer.child_spec(log_opts))
    {:ok, pid}
  end

  @doc """
  Cleans up a test log server and its files.
  """
  @spec cleanup_test_log(pid()) :: :ok
  def cleanup_test_log(log_pid) do
    if Process.alive?(log_pid) do
      GenServer.stop(log_pid)
    end

    :ok
  end

  @doc """
  Pushes multiple transactions to a log in sequence, handling version tracking.
  Assumes the log is already unlocked.
  """
  @spec push_transactions_to_log(pid(), [Transaction.encoded()]) :: :ok | {:error, term()}
  def push_transactions_to_log(log_pid, transactions) do
    last_version = Version.from_integer(0)

    transactions
    |> Enum.reduce_while(last_version, fn tx, prev_version ->
      case Log.push(log_pid, tx, prev_version) do
        :ok ->
          {:ok, version} = Transaction.commit_version(tx)
          {:cont, version}

        error ->
          {:halt, error}
      end
    end)
    |> case do
      {:error, _} = error -> error
      _ -> :ok
    end
  end

  @doc """
  Gets information from a log using the Log module interface.
  """
  @spec get_log_info(pid(), [atom()]) :: {:ok, map()} | {:error, term()}
  def get_log_info(log_pid, info_keys) do
    Log.info(log_pid, info_keys)
  end
end
