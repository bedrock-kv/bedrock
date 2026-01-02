defmodule Bedrock.JobQueue.Consumer.Worker do
  @moduledoc """
  Processes individual jobs.

  Workers execute job modules' perform/1 callbacks with timeout protection.
  Results are reported back to the Manager for completion/requeue handling.
  """

  use GenServer, restart: :temporary

  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Registry

  require Logger

  defstruct [:item, :lease, :registry, :reply_to, :task_ref]

  def start_link(job) do
    GenServer.start_link(__MODULE__, job)
  end

  @impl true
  def init(job) do
    state = %__MODULE__{
      item: job.item,
      lease: job.lease,
      registry: job.registry,
      reply_to: job.reply_to
    }

    {:ok, state, {:continue, :execute}}
  end

  @impl true
  def handle_continue(:execute, state) do
    task =
      Task.async(fn ->
        execute_job(state.item, state.registry)
      end)

    {:noreply, %{state | task_ref: task.ref}}
  end

  @impl true
  def handle_info({ref, result}, %{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    send(state.reply_to, {:worker_done, state.lease, result})
    {:stop, :normal, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task_ref: ref} = state) do
    Logger.error("Job #{state.item.id} crashed: #{inspect(reason)}")
    send(state.reply_to, {:worker_done, state.lease, {:error, {:crash, reason}}})
    {:stop, :normal, state}
  end

  defp execute_job(%Item{} = item, registry) do
    case Registry.lookup(registry, item.topic) do
      {:ok, job_module} ->
        execute_with_timeout(job_module, item)

      :error ->
        Logger.warning("No handler registered for topic: #{item.topic}")
        {:discard, :no_handler}
    end
  end

  defp execute_with_timeout(job_module, item) do
    timeout = get_timeout(job_module)
    args = decode_payload(item.payload)

    task =
      Task.async(fn ->
        job_module.perform(args)
      end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} ->
        result

      nil ->
        {:error, :timeout}

      {:exit, reason} ->
        {:error, {:exit, reason}}
    end
  rescue
    e ->
      Logger.error("Job execution failed: #{Exception.message(e)}")
      {:error, {:exception, e}}
  end

  defp get_timeout(job_module) do
    if function_exported?(job_module, :timeout, 0) do
      job_module.timeout()
    else
      30_000
    end
  end

  defp decode_payload(payload) when is_binary(payload) do
    case Jason.decode(payload, keys: :atoms) do
      {:ok, decoded} -> decoded
      {:error, _} -> %{raw: payload}
    end
  end

  defp decode_payload(payload), do: payload
end
