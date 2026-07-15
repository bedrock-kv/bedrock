defmodule Bedrock.ControlPlane.Distributor.DeathHealingTest do
  @moduledoc """
  Unit tests for materializer death healing (bedrock-q67.7): the
  Distributor monitors recruited and recovery-provided materializers; on a
  materializer's death it swaps the placeholder into the TSL slot
  (epoch-guarded), uncovers the tag at the placeholder so requests park
  and re-demand instead of forwarding to the corpse, and eagerly
  re-recruits under the existing per-tag failure backoff.
  """
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Distributor
  alias Bedrock.ControlPlane.Distributor.Placeholder
  alias Bedrock.ControlPlane.Distributor.State
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.ControlPlane.StubMaterializer

  defmodule TestCluster do
    @moduledoc false
    def otp_name(component), do: :"death_healing_#{component}"
  end

  defmodule CaptureDirector do
    @moduledoc false
    use GenServer

    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)

    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_call({:apply_tsl_delta, delta, epoch}, _from, test_pid) do
      send(test_pid, {:apply_tsl_delta, delta, epoch})
      {:reply, :ok, test_pid}
    end
  end

  # A single data shard (tag 1) covering the whole keyspace.
  @shard_layout %{<<0xFF, 0xFF>> => {1, <<>>}}
  @version Version.from_integer(1)
  @epoch 42

  defp unique_otp_name, do: :"death_healing_#{System.unique_integer([:positive])}"

  defp attach_telemetry(test_pid) do
    handler_id = "death-healing-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      [
        [:bedrock, :distributor, :materializer_down],
        [:bedrock, :distributor, :healing, :started],
        [:bedrock, :distributor, :healing, :completed],
        [:bedrock, :distributor, :recruitment, :failed],
        [:bedrock, :distributor, :placeholder, :parked],
        [:bedrock, :distributor, :placeholder, :forwarded]
      ],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp start_stub(kvs) do
    start_supervised!(%{
      id: {StubMaterializer, System.unique_integer([:positive])},
      start: {StubMaterializer, :start_link, [kvs]},
      restart: :temporary
    })
  end

  defp start_distributor(opts) do
    director = start_supervised!({CaptureDirector, self()}, id: :capture_director)

    pid =
      start_supervised!(
        Distributor.child_spec(
          Keyword.merge(
            [
              cluster: TestCluster,
              epoch: @epoch,
              director: director,
              shard_layout: @shard_layout,
              node_capabilities: %{materializer: [node()]},
              durable_version: Version.zero(),
              otp_name: unique_otp_name()
            ],
            opts
          )
        )
      )

    {pid, director}
  end

  # Recruitment seams that hand out a queue of stub materializer pids, one
  # per recruitment attempt, notifying the test of every worker creation.
  defp queued_recruitment(test_pid, stubs) do
    {:ok, queue} = Agent.start_link(fn -> stubs end)

    %{
      create_worker_fn: fn _foreman, _worker_id, :materializer, _opts ->
        send(test_pid, :create_worker_called)

        case Agent.get(queue, & &1) do
          [] -> {:error, :no_capacity}
          [_next | _rest] -> {:ok, :stub_worker_ref}
        end
      end,
      lock_materializer_fn: fn _worker, _epoch ->
        {:ok, Agent.get_and_update(queue, fn [next | rest] -> {next, rest} end), %{}}
      end,
      unlock_materializer_fn: fn _pid, _durable_version, _tsl -> :ok end
    }
  end

  defp placeholder_of(distributor) do
    %State{placeholder: placeholder} = :sys.get_state(distributor)
    placeholder
  end

  defp park_read(placeholder, key, timeout \\ 5_000) do
    Task.async(fn -> Materializer.get(placeholder, key, @version, timeout: timeout) end)
  end

  defp wait_until(fun, deadline_ms \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + deadline_ms

    fn ->
      if fun.() do
        :ok
      else
        if System.monotonic_time(:millisecond) > deadline, do: flunk("wait_until timed out")
        Process.sleep(10)
        :retry
      end
    end
    |> Stream.repeatedly()
    |> Enum.find(&(&1 == :ok))
  end

  defp recruit_initial(distributor, stub) do
    task = park_read(placeholder_of(distributor), "apple")
    assert_receive {:apply_tsl_delta, %{1 => ^stub}, @epoch}, 5_000
    assert {:ok, "red"} = Task.await(task, 5_000)
  end

  describe "monitoring" do
    test "pre-existing data-shard materializers from the TSL snapshot are monitored; the system shard is not" do
      system_stub = start_stub(%{})
      data_stub = start_stub(%{})

      {distributor, _director} =
        start_distributor(
          transaction_system_layout: %{
            shard_materializers: %{0 => system_stub, 1 => data_stub}
          }
        )

      assert %State{materializer_monitors: monitors} = :sys.get_state(distributor)
      assert [{1, ^data_stub}] = Map.values(monitors)
    end

    test "a successfully recruited materializer is monitored" do
      stub = start_stub(%{"apple" => "red"})
      {distributor, _director} = start_distributor(recruitment: queued_recruitment(self(), [stub]))

      recruit_initial(distributor, stub)

      wait_until(fn ->
        %State{materializer_monitors: monitors} = :sys.get_state(distributor)
        Map.values(monitors) == [{1, stub}]
      end)
    end
  end

  describe "healing on materializer death" do
    test "death of a pre-existing materializer swaps the placeholder into the slot and re-recruits" do
      attach_telemetry(self())
      dying = start_stub(%{})
      replacement = start_stub(%{"apple" => "red"})

      {distributor, _director} =
        start_distributor(
          recruitment: queued_recruitment(self(), [replacement]),
          transaction_system_layout: %{shard_materializers: %{1 => dying}}
        )

      placeholder = placeholder_of(distributor)

      Process.exit(dying, :kill)

      assert_receive {:telemetry, [:bedrock, :distributor, :materializer_down], %{},
                      %{cluster: TestCluster, epoch: @epoch, tag: 1, reason: :killed}}

      assert_receive {:telemetry, [:bedrock, :distributor, :healing, :started], %{}, %{tag: 1}}

      # The placeholder is swapped into the dead materializer's slot...
      assert_receive {:apply_tsl_delta, %{1 => ^placeholder}, @epoch}, 5_000

      # ...and re-recruitment runs eagerly, restoring real coverage.
      assert_receive :create_worker_called, 5_000
      assert_receive {:apply_tsl_delta, %{1 => ^replacement}, @epoch}, 5_000
      assert_receive {:telemetry, [:bedrock, :distributor, :healing, :completed], %{}, %{tag: 1}}, 5_000

      # The healed materializer is monitored and the healing set is clear.
      wait_until(fn ->
        %State{materializer_monitors: monitors, healing: healing} = :sys.get_state(distributor)
        Map.values(monitors) == [{1, replacement}] and MapSet.size(healing) == 0
      end)
    end

    test "death of a recruited materializer uncovers the tag: the next read parks instead of forwarding" do
      attach_telemetry(self())
      dying = start_stub(%{"apple" => "red"})
      replacement = start_stub(%{"apple" => "blue"})
      test_pid = self()
      {:ok, queue} = Agent.start_link(fn -> [dying, replacement] end)

      # Worker creation blocks until the test releases it, so the state of
      # the placeholder between death and re-recruitment is observable.
      recruitment = %{
        create_worker_fn: fn _foreman, _worker_id, :materializer, _opts ->
          send(test_pid, {:creating, self()})

          receive do
            :release -> {:ok, :stub_worker_ref}
          end
        end,
        lock_materializer_fn: fn _worker, _epoch ->
          {:ok, Agent.get_and_update(queue, fn [next | rest] -> {next, rest} end), %{}}
        end,
        unlock_materializer_fn: fn _pid, _durable_version, _tsl -> :ok end
      }

      {distributor, _director} = start_distributor(recruitment: recruitment)
      placeholder = placeholder_of(distributor)

      task = park_read(placeholder, "apple")
      assert_receive {:creating, first_attempt}, 5_000
      send(first_attempt, :release)
      assert_receive {:apply_tsl_delta, %{1 => ^dying}, @epoch}, 5_000
      assert {:ok, "red"} = Task.await(task, 5_000)

      # A stale-layout read forwards through the placeholder while covered.
      forwarded = park_read(placeholder, "apple")
      assert {:ok, "red"} = Task.await(forwarded, 5_000)
      assert_receive {:telemetry, [:bedrock, :distributor, :placeholder, :forwarded], %{}, %{tag: 1}}

      Process.exit(dying, :kill)
      assert_receive {:apply_tsl_delta, %{1 => ^placeholder}, @epoch}, 5_000
      assert_receive {:creating, second_attempt}, 5_000

      # The placeholder's covered entry for the dead pid is cleared - no
      # stale-forward black hole.
      wait_until(fn -> :sys.get_state(placeholder).covered == %{} end)

      # The next read PARKS (a forward to the corpse could never produce a
      # value) and drains through the replacement once re-recruitment is
      # released and completes.
      task = park_read(placeholder, "apple")
      wait_until(fn -> map_size(:sys.get_state(placeholder).waiting) > 0 end)

      send(second_attempt, :release)
      assert {:ok, "blue"} = Task.await(task, 5_000)
      assert_receive {:apply_tsl_delta, %{1 => ^replacement}, @epoch}, 5_000
    end

    test "the failure backoff is respected when re-recruitment after death fails" do
      attach_telemetry(self())
      dying = start_stub(%{"apple" => "red"})

      {distributor, _director} =
        start_distributor(recruitment: queued_recruitment(self(), [dying]), backoff_ms: 400)

      recruit_initial(distributor, dying)
      assert_receive :create_worker_called
      placeholder = placeholder_of(distributor)

      # Death triggers one eager re-recruitment attempt, which fails (the
      # queue is exhausted) and opens the backoff window.
      Process.exit(dying, :kill)
      assert_receive :create_worker_called, 5_000
      assert_receive {:telemetry, [:bedrock, :distributor, :recruitment, :failed], %{}, %{tag: 1}}, 5_000

      # A read inside the backoff window is shed promptly with :unavailable
      # and does NOT start another recruitment attempt.
      task = park_read(placeholder, "apple")
      assert {:error, :unavailable} = Task.await(task, 5_000)
      refute_receive :create_worker_called, 100

      # Once the backoff expires, the next read re-demands and recruitment
      # is retried.
      Process.sleep(400)
      task = park_read(placeholder, "apple")
      assert_receive :create_worker_called, 5_000
      assert {:error, :unavailable} = Task.await(task, 5_000)
    end

    test "a stale swap-complete for a tag already re-covered does not recruit a duplicate" do
      stub = start_stub(%{"apple" => "red"})
      {distributor, _director} = start_distributor(recruitment: queued_recruitment(self(), [stub]))

      recruit_initial(distributor, stub)
      assert_receive :create_worker_called

      # Simulate the race: a demand issued between the DOWN and the swap's
      # completion recruited coverage before {:placeholder_swap_complete, ...}
      # was processed. The stale swap-complete must not start a second
      # recruitment (which would orphan the live recruit).
      GenServer.cast(distributor, {:placeholder_swap_complete, 1, :ok})

      refute_receive :create_worker_called, 100

      %State{materializer_monitors: monitors} = :sys.get_state(distributor)
      assert Map.values(monitors) == [{1, stub}]
    end

    test "placeholder restart during healing re-points healed slots at the new placeholder" do
      dying = start_stub(%{})
      test_pid = self()

      # Recruitment stalls in flight so the tag stays in healing.
      recruitment = %{
        create_worker_fn: fn _foreman, _worker_id, :materializer, _opts ->
          send(test_pid, {:blocked_recruitment, self()})

          receive do
            :release -> {:error, :no_capacity}
          end
        end
      }

      {distributor, _director} =
        start_distributor(
          recruitment: recruitment,
          transaction_system_layout: %{shard_materializers: %{1 => dying}}
        )

      placeholder = placeholder_of(distributor)

      Process.exit(dying, :kill)
      assert_receive {:apply_tsl_delta, %{1 => ^placeholder}, @epoch}, 5_000
      assert_receive {:blocked_recruitment, blocked}, 5_000

      Process.exit(placeholder, :kill)

      wait_until(fn -> placeholder_of(distributor) != placeholder end)
      new_placeholder = placeholder_of(distributor)

      assert_receive {:apply_tsl_delta, %{1 => ^new_placeholder}, @epoch}, 5_000

      send(blocked, :release)
    end
  end

  describe "deliver-races-restart" do
    test "a re-demand after a lost coverage delivery re-delivers the live materializer without recruiting again" do
      stub = start_stub(%{"apple" => "red"})
      {distributor, _director} = start_distributor(recruitment: queued_recruitment(self(), [stub]))

      recruit_initial(distributor, stub)
      assert_receive :create_worker_called
      placeholder = placeholder_of(distributor)

      # Simulate the race: the placeholder restarts having never received
      # the `notify_covered` for the completed recruitment.
      Process.exit(placeholder, :kill)
      wait_until(fn -> placeholder_of(distributor) != placeholder end)
      new_placeholder = placeholder_of(distributor)

      # A read against the restarted placeholder parks and re-demands; the
      # distributor re-delivers its known-live coverage instead of
      # recruiting a duplicate materializer.
      task = park_read(new_placeholder, "apple")
      assert {:ok, "red"} = Task.await(task, 5_000)
      refute_receive :create_worker_called, 100
    end
  end

  describe "Placeholder.notify_uncovered/2" do
    test "clears coverage so requests park and re-demand instead of forwarding" do
      placeholder =
        start_supervised!(
          Placeholder.Server.child_spec(
            cluster: TestCluster,
            distributor: self(),
            shard_layout: @shard_layout,
            hold_ms: 2_000
          )
        )

      stub = start_stub(%{"apple" => "red"})

      :ok = Placeholder.notify_covered(placeholder, 1, stub)
      task = park_read(placeholder, "apple")
      assert {:ok, "red"} = Task.await(task, 5_000)
      refute_receive {:"$gen_cast", {:coverage_demand, 1}}, 100

      :ok = Placeholder.notify_uncovered(placeholder, 1)

      task = park_read(placeholder, "apple")
      assert_receive {:"$gen_cast", {:coverage_demand, 1}}, 5_000

      :ok = Placeholder.notify_covered(placeholder, 1, stub)
      assert {:ok, "red"} = Task.await(task, 5_000)
    end
  end
end
