defmodule Bedrock.Test.History.Driver do
  @moduledoc "Runs and records individual public Repo attempts without hiding ambiguous retries."
  alias Bedrock.DataPlane.Transaction

  def start_recorder do
    {revision, _} = System.cmd("git", ["rev-parse", "HEAD"], stderr_to_stdout: true)

    Agent.start_link(fn ->
      %{
        attempts: %{},
        batches: [],
        faults: [],
        initial: %{},
        seed: 239,
        revision: String.trim(revision),
        exunit_seed: Application.get_env(:ex_unit, :seed)
      }
    end)
  end

  def attach(recorder) do
    id = {__MODULE__, recorder}
    :ok = :telemetry.attach(id, [:bedrock, :log, :push], &__MODULE__.log_event/4, recorder)
    id
  end

  def log_event(_event, _measurements, %{transaction: encoded}, recorder) do
    {:ok, decoded} = Transaction.decode(encoded)
    ids = for {:set, "history/meta/" <> id, _} <- decoded.mutations, do: id

    if ids != [] do
      Agent.update(recorder, fn state ->
        %{state | batches: [%{version: decoded.commit_version, ids: ids} | state.batches]}
      end)
    end
  end

  def attempt(repo, recorder, id, operations, opts \\ []) do
    invoke = System.monotonic_time()
    Process.delete({__MODULE__, :observations})
    marker = {:put, "history/meta/" <> id, "attempt"}
    ops = operations ++ [marker]

    pending = %{
      id: id,
      invoke: invoke,
      complete: nil,
      status: :in_flight,
      ops: ops,
      reads: [],
      callback_complete: false,
      error: nil
    }

    Agent.update(recorder, &put_in(&1, [:attempts, id], pending))

    observe = fn observation ->
      Agent.update(recorder, &update_in(&1, [:attempts, id, :reads], fn reads -> reads ++ [observation] end))
    end

    opts = Keyword.put(opts, :observe, observe)

    {status, error} =
      try do
        result =
          repo.transact(
            fn ->
              observations = execute(repo, operations, opts)
              repo.put("history/meta/" <> id, "attempt")
              Process.put({__MODULE__, :observations}, observations)
              Agent.update(recorder, &put_in(&1, [:attempts, id, :callback_complete], true))
              observations
            end,
            retry_limit: 0,
            timeout_in_ms: Keyword.get(opts, :timeout_in_ms, 5_000)
          )

        {classify_return(result, Process.get({__MODULE__, :observations})), nil}
      rescue
        exception ->
          message = Exception.message(exception)
          status = classify_exception(exception, Process.get({__MODULE__, :observations}))
          {status, message}
      catch
        kind, reason ->
          status = if Process.get({__MODULE__, :observations}) == nil, do: :aborted, else: :unknown
          {status, inspect({kind, reason})}
      end

    entry = %{
      id: id,
      invoke: invoke,
      complete: System.monotonic_time(),
      status: status,
      ops: ops,
      reads: Agent.get(recorder, & &1.attempts[id].reads),
      callback_complete: Process.get({__MODULE__, :observations}) != nil,
      error: error
    }

    Agent.update(recorder, &put_in(&1, [:attempts, id], entry))
    entry
  end

  def classify_return(result, observations) when is_list(observations) and result == observations, do: :committed
  def classify_return({:error, _reason}, _observations), do: :aborted
  def classify_return(_result, _observations), do: :unknown

  def classify_exception(_exception, nil), do: :aborted

  def classify_exception(
        %RuntimeError{message: "Transaction retry limit exceeded after 0 attempts. Last error: :aborted"},
        _observations
      ), do: :aborted

  def classify_exception(_exception, _observations), do: :unknown

  def metadata(recorder, fields), do: Agent.update(recorder, &Map.merge(&1, fields))

  def execute(repo, operations, opts \\ []) do
    operations
    |> Enum.reduce([], fn op, observations ->
      case execute_operation(repo, op) do
        :mutation ->
          observations

        observation ->
          Keyword.get(opts, :observe, fn _ -> :ok end).(observation)
          Keyword.get(opts, :after_read, fn _ -> :ok end).(observation)
          [observation | observations]
      end
    end)
    |> Enum.reverse()
  end

  defp execute_operation(repo, {:put, key, value}),
    do:
      (
        repo.put(key, value)
        :mutation
      )

  defp execute_operation(repo, {:clear, key}),
    do:
      (
        repo.clear(key)
        :mutation
      )

  defp execute_operation(repo, {:clear_range, first, last}),
    do:
      (
        repo.clear_range({first, last})
        :mutation
      )

  defp execute_operation(repo, {:add, key, amount}),
    do:
      (
        repo.add(key, <<amount::64-little>>)
        :mutation
      )

  defp execute_operation(repo, {:get, key}), do: {:get, key, repo.get(key)}
  defp execute_operation(repo, {:range, first, last}), do: {:range, Enum.to_list(repo.get_range({first, last}))}

  defp execute_operation(repo, {:reserve, range, key}) do
    empty = Enum.to_list(repo.get_range(range)) == []
    if empty, do: repo.put(key, "reserved")
    {:reserve, empty}
  end

  defp execute_operation(repo, {:transfer, from, to, amount}) do
    source = repo.get(from)
    balance = number(source)
    enough = balance >= amount
    destination = if enough, do: repo.get(to), else: :unread

    if enough do
      repo.put(from, <<balance - amount::64-little>>)
      repo.put(to, <<number(destination) + amount::64-little>>)
    end

    {:transfer, source, destination, enough}
  end

  defp number(nil), do: 0
  defp number(<<value::64-little>>), do: value

  def artifact(recorder, scenario, extra \\ %{}) do
    directory =
      System.get_env("BEDROCK_HISTORY_ARTIFACT_DIR") || Path.join(System.tmp_dir!(), "bedrock-history-artifacts")

    File.mkdir_p!(directory)
    path = Path.join(directory, "#{scenario}-#{System.unique_integer([:positive])}.term")
    metadata(recorder, extra)
    data = Agent.get(recorder, & &1)
    File.write!(path, :erlang.term_to_binary(data))
    File.write!(path <> ".txt", inspect(data, pretty: true, limit: :infinity, printable_limit: :infinity))
    path
  end
end
