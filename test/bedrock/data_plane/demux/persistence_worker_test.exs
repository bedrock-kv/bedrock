defmodule Bedrock.DataPlane.Demux.PersistenceWorkerTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Demux.PersistenceWorker

  describe "enqueue/2" do
    test "processes payloads in enqueue order" do
      test_pid = self()

      {:ok, worker} =
        start_supervised(
          {PersistenceWorker,
           perform: fn payload ->
             send(test_pid, {:processed, payload})
             :ok
           end}
        )

      assert :ok = PersistenceWorker.enqueue(worker, :a)
      assert :ok = PersistenceWorker.enqueue(worker, :b)
      assert :ok = PersistenceWorker.enqueue(worker, :c)

      assert_receive {:processed, :a}, 1_000
      assert_receive {:processed, :b}, 1_000
      assert_receive {:processed, :c}, 1_000

      eventually(fn ->
        PersistenceWorker.stats(worker) == %{pending: 0, scheduled: 0, in_flight: 0, lag: 0}
      end)
    end

    test "retries failed payloads and eventually succeeds" do
      test_pid = self()
      {:ok, attempts} = Agent.start_link(fn -> %{} end)

      {:ok, worker} =
        start_supervised(
          {PersistenceWorker,
           max_retries: 3,
           retry_base_backoff_ms: 1,
           retry_tick_ms: 1,
           perform: fn payload ->
             attempt =
               Agent.get_and_update(attempts, fn state ->
                 current = Map.get(state, payload, 0) + 1
                 {current, Map.put(state, payload, current)}
               end)

             send(test_pid, {:attempt, payload, attempt})

             if attempt == 1 do
               {:error, :transient}
             else
               :ok
             end
           end}
        )

      assert :ok = PersistenceWorker.enqueue(worker, :retry_item)

      assert_receive {:attempt, :retry_item, 1}, 1_000
      assert_receive {:attempt, :retry_item, 2}, 1_000

      eventually(fn ->
        PersistenceWorker.stats(worker) == %{pending: 0, scheduled: 0, in_flight: 0, lag: 0}
      end)
    end
  end

  defp eventually(fun, retries \\ 40)

  defp eventually(fun, retries) when retries > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      eventually(fun, retries - 1)
    end
  end

  defp eventually(_fun, 0), do: flunk("condition not met in allotted retries")
end
