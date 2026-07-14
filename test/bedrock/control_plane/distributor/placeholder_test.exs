defmodule Bedrock.ControlPlane.Distributor.PlaceholderTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Distributor.Placeholder
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.DataPlane.Version
  alias Bedrock.KeySelector
  alias Bedrock.Test.ControlPlane.StubMaterializer

  defmodule TestCluster do
    @moduledoc false
  end

  # Layout: tag 1 covers [<<>>, "m"), tag 2 covers ["m", <<0xFF, 0xFF>>).
  @shard_layout %{
    "m" => {1, <<>>},
    <<0xFF, 0xFF>> => {2, "m"}
  }

  @version Version.from_integer(1)

  defp start_placeholder(opts \\ []) do
    start_supervised!(
      Placeholder.Server.child_spec(
        cluster: TestCluster,
        distributor: Keyword.get(opts, :distributor, self()),
        shard_layout: Keyword.get(opts, :shard_layout, @shard_layout),
        hold_ms: Keyword.get(opts, :hold_ms, 2_000)
      )
    )
  end

  defp start_stub(kvs) do
    start_supervised!(%{
      id: {StubMaterializer, System.unique_integer([:positive])},
      start: {StubMaterializer, :start_link, [kvs]}
    })
  end

  defp async_get(placeholder, key, opts \\ []),
    do: Task.async(fn -> Materializer.get(placeholder, key, @version, opts) end)

  defp attach_telemetry(test_pid) do
    handler_id = "placeholder-test-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      [
        [:bedrock, :distributor, :placeholder, :parked],
        [:bedrock, :distributor, :placeholder, :forwarded],
        [:bedrock, :distributor, :placeholder, :drained],
        [:bedrock, :distributor, :placeholder, :shed]
      ],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  describe "park then cover" do
    test "parked get is drained with the real value once coverage arrives" do
      attach_telemetry(self())
      placeholder = start_placeholder()
      stub = start_stub(%{"apple" => "red"})

      task = async_get(placeholder, "apple", timeout: 1_000)

      assert_receive {:"$gen_cast", {:coverage_demand, 1}}
      assert_receive {:telemetry, [:bedrock, :distributor, :placeholder, :parked], %{count: 1}, %{tag: 1}}

      :ok = Placeholder.notify_covered(placeholder, 1, stub)

      assert {:ok, "red"} = Task.await(task, 1_000)
      assert_receive {:telemetry, [:bedrock, :distributor, :placeholder, :drained], %{count: 1}, %{tag: 1}}
    end

    test "parked key-selector get is drained with the resolved key-value" do
      placeholder = start_placeholder()
      stub = start_stub(%{"apple" => "red"})

      selector = KeySelector.first_greater_or_equal("apple")
      task = Task.async(fn -> Materializer.get(placeholder, selector, @version, timeout: 1_000) end)

      assert_receive {:"$gen_cast", {:coverage_demand, 1}}
      :ok = Placeholder.notify_covered(placeholder, 1, stub)

      assert {:ok, {"apple", "red"}} = Task.await(task, 1_000)
    end

    test "parked get_range is drained with the shard's key-values" do
      placeholder = start_placeholder()
      stub = start_stub(%{"apple" => "red", "banana" => "yellow", "zebra" => "striped"})

      task = Task.async(fn -> Materializer.get_range(placeholder, "a", "c", @version, timeout: 1_000) end)

      assert_receive {:"$gen_cast", {:coverage_demand, 1}}
      :ok = Placeholder.notify_covered(placeholder, 1, stub)

      assert {:ok, {[{"apple", "red"}, {"banana", "yellow"}], false}} = Task.await(task, 1_000)
    end

    test "draining multiple parked requests replies to every waiter" do
      placeholder = start_placeholder()
      stub = start_stub(%{"apple" => "red", "cherry" => "dark red"})

      task_a = async_get(placeholder, "apple", timeout: 1_000)
      task_b = async_get(placeholder, "cherry", timeout: 1_000)

      assert_receive {:"$gen_cast", {:coverage_demand, 1}}
      :ok = Placeholder.notify_covered(placeholder, 1, stub)

      assert {:ok, "red"} = Task.await(task_a, 1_000)
      assert {:ok, "dark red"} = Task.await(task_b, 1_000)
    end
  end

  describe "deadline expiry" do
    test "parked request is shed with :unavailable when hold_ms expires" do
      attach_telemetry(self())
      placeholder = start_placeholder(hold_ms: 30)

      task = async_get(placeholder, "apple", timeout: 1_000)

      assert {:error, :unavailable} = Task.await(task, 1_000)

      assert_receive {:telemetry, [:bedrock, :distributor, :placeholder, :shed], %{count: 1},
                      %{reason: :deadline_expired}}
    end

    test "caller-supplied timeout below hold_ms bounds the parking budget" do
      placeholder = start_placeholder(hold_ms: 5_000)

      # Call directly with a generous GenServer timeout so the placeholder's
      # budget (opts[:timeout] = 30ms) is what trips, not the client call.
      task =
        Task.async(fn ->
          GenServer.call(placeholder, {:get, "apple", @version, [timeout: 30]}, 1_000)
        end)

      assert {:error, :unavailable} = Task.await(task, 1_000)
    end

    test "expiry only sheds entries past their deadline" do
      placeholder = start_placeholder(hold_ms: 5_000)
      stub = start_stub(%{"apple" => "red"})

      short_task =
        Task.async(fn ->
          GenServer.call(placeholder, {:get, "apple", @version, [timeout: 30]}, 1_000)
        end)

      long_task = async_get(placeholder, "apple", timeout: 5_000)

      assert {:error, :unavailable} = Task.await(short_task, 1_000)

      # The longer-deadline waiter is still parked and drains on coverage.
      :ok = Placeholder.notify_covered(placeholder, 1, stub)
      assert {:ok, "red"} = Task.await(long_task, 1_000)
    end
  end

  describe "forwarding" do
    test "requests for a covered tag are forwarded without parking" do
      attach_telemetry(self())
      placeholder = start_placeholder()
      stub = start_stub(%{"apple" => "red"})

      :ok = Placeholder.notify_covered(placeholder, 1, stub)

      assert {:ok, "red"} = Materializer.get(placeholder, "apple", @version, timeout: 1_000)
      assert_receive {:telemetry, [:bedrock, :distributor, :placeholder, :forwarded], %{count: 1}, %{tag: 1}}
      refute_receive {:"$gen_cast", {:coverage_demand, _tag}}, 50
    end

    test "forwarding is per-tag: uncovered tags still park" do
      placeholder = start_placeholder()
      stub = start_stub(%{"apple" => "red"})

      :ok = Placeholder.notify_covered(placeholder, 1, stub)

      task = async_get(placeholder, "pear", timeout: 1_000)
      assert_receive {:"$gen_cast", {:coverage_demand, 2}}

      :ok = Placeholder.notify_covered(placeholder, 2, start_stub(%{"pear" => "green"}))
      assert {:ok, "green"} = Task.await(task, 1_000)
    end
  end

  describe "demand dedupe" do
    test "at most one coverage demand per tag until covered" do
      placeholder = start_placeholder()

      task_a = async_get(placeholder, "apple", timeout: 1_000)
      task_b = async_get(placeholder, "banana", timeout: 1_000)

      assert_receive {:"$gen_cast", {:coverage_demand, 1}}
      refute_receive {:"$gen_cast", {:coverage_demand, 1}}, 50

      stub = start_stub(%{"apple" => "red", "banana" => "yellow"})
      :ok = Placeholder.notify_covered(placeholder, 1, stub)
      assert {:ok, "red"} = Task.await(task_a, 1_000)
      assert {:ok, "yellow"} = Task.await(task_b, 1_000)
    end

    test "distinct tags each signal their own demand" do
      placeholder = start_placeholder(hold_ms: 30)

      task_a = async_get(placeholder, "apple", timeout: 1_000)
      task_b = async_get(placeholder, "pear", timeout: 1_000)

      assert_receive {:"$gen_cast", {:coverage_demand, 1}}
      assert_receive {:"$gen_cast", {:coverage_demand, 2}}

      assert {:error, :unavailable} = Task.await(task_a, 1_000)
      assert {:error, :unavailable} = Task.await(task_b, 1_000)
    end
  end

  describe "coverage failure" do
    test "sheds parked requests with :unavailable and re-arms the demand" do
      attach_telemetry(self())
      placeholder = start_placeholder()

      task = async_get(placeholder, "apple", timeout: 1_000)
      assert_receive {:"$gen_cast", {:coverage_demand, 1}}

      :ok = Placeholder.notify_coverage_failed(placeholder, 1, :no_capacity)

      assert {:error, :unavailable} = Task.await(task, 1_000)

      assert_receive {:telemetry, [:bedrock, :distributor, :placeholder, :shed], %{count: 1},
                      %{tag: 1, reason: :no_capacity}}

      # Dedupe was cleared: a later request re-triggers the demand.
      retry_task = async_get(placeholder, "apple", timeout: 1_000)
      assert_receive {:"$gen_cast", {:coverage_demand, 1}}

      stub = start_stub(%{"apple" => "red"})
      :ok = Placeholder.notify_covered(placeholder, 1, stub)
      assert {:ok, "red"} = Task.await(retry_task, 1_000)
    end
  end

  describe "layout misses" do
    test "keys outside every shard are refused with :unavailable" do
      placeholder = start_placeholder(shard_layout: %{"m" => {1, "a"}})

      assert {:error, :unavailable} = GenServer.call(placeholder, {:get, "zebra", @version, []}, 1_000)
      refute_receive {:"$gen_cast", {:coverage_demand, _tag}}, 50
    end
  end
end
