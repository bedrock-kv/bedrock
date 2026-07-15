defmodule Bedrock.ControlPlane.Distributor.RecruitmentTest do
  @moduledoc """
  Unit tests for the Distributor's on-demand materializer recruitment flow,
  with the worker layer stubbed through the same injectable-function seams
  the recovery bootstrap phase uses (`create_worker_fn`,
  `lock_materializer_fn`, `unlock_materializer_fn`).
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Distributor
  alias Bedrock.ControlPlane.Distributor.State
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.ControlPlane.StubMaterializer

  defmodule TestCluster do
    @moduledoc false
    def otp_name(component), do: :"distributor_recruitment_unit_#{component}"
  end

  defmodule CaptureDirector do
    @moduledoc """
    A stand-in director that captures `apply_tsl_delta` calls, forwards them
    to the test process, and replies with a configured result.
    """
    use GenServer

    def start_link(test_pid, reply \\ :ok), do: GenServer.start_link(__MODULE__, {test_pid, reply})

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call({:apply_tsl_delta, delta, epoch}, _from, {test_pid, reply} = state) do
      send(test_pid, {:apply_tsl_delta, delta, epoch})
      {:reply, reply, state}
    end
  end

  # A single data shard (tag 1) covering the whole keyspace.
  @shard_layout %{<<0xFF, 0xFF>> => {1, <<>>}}
  @version Version.from_integer(1)

  defp unique_otp_name, do: :"distributor_recruitment_unit_#{System.unique_integer([:positive])}"

  defp attach_recruitment_telemetry(test_pid) do
    handler_id = "recruitment-test-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      [
        [:bedrock, :distributor, :recruitment, :started],
        [:bedrock, :distributor, :recruitment, :succeeded],
        [:bedrock, :distributor, :recruitment, :failed]
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
      start: {StubMaterializer, :start_link, [kvs]}
    })
  end

  defp start_distributor(opts) do
    director =
      Keyword.get_lazy(opts, :director, fn -> start_supervised!({CaptureDirector, self()}, id: :capture_director) end)

    pid =
      start_supervised!(
        Distributor.child_spec(
          Keyword.merge(
            [
              cluster: TestCluster,
              epoch: 42,
              director: director,
              shard_layout: @shard_layout,
              node_capabilities: %{materializer: [node()]},
              durable_version: Version.zero(),
              otp_name: unique_otp_name()
            ],
            Keyword.delete(opts, :director)
          )
        )
      )

    {pid, director}
  end

  defp placeholder_of(distributor) do
    %State{placeholder: placeholder} = :sys.get_state(distributor)
    placeholder
  end

  defp park_read(placeholder, key) do
    Task.async(fn -> Materializer.get(placeholder, key, @version, timeout: 5_000) end)
  end

  describe "successful recruitment" do
    test "demand drives placement, foreman creation, epoch lock/unlock, TSL delta, and coverage" do
      attach_recruitment_telemetry(self())
      test_pid = self()
      stub = start_stub(%{"apple" => "red"})

      recruitment = %{
        create_worker_fn: fn foreman, worker_id, kind, _opts ->
          send(test_pid, {:create_worker, foreman, worker_id, kind})
          {:ok, :stub_worker_ref}
        end,
        lock_materializer_fn: fn worker, epoch ->
          send(test_pid, {:lock, worker, epoch})

          recovery_info = %{
            kind: :materializer,
            durable_version: Version.zero(),
            oldest_durable_version: Version.zero()
          }

          {:ok, stub, recovery_info}
        end,
        unlock_materializer_fn: fn pid, durable_version, tsl ->
          send(test_pid, {:unlock, pid, durable_version, tsl})
          :ok
        end
      }

      snapshot = %{
        logs: %{"log_a" => [1], "log_b" => [2]},
        services: %{"svc" => %{kind: :log}}
      }

      # The cluster-wide recovery durable version is far ahead of the fresh
      # worker's (empty) store; the unlock below must use the version the
      # worker itself reported at lock time, not this one.
      {distributor, _director} =
        start_distributor(
          recruitment: recruitment,
          transaction_system_layout: snapshot,
          durable_version: Version.from_integer(500)
        )

      task = park_read(placeholder_of(distributor), "apple")

      # Placement + foreman boundary: worker created via the chosen node's foreman.
      expected_foreman = {TestCluster.otp_name(:foreman), node()}
      assert_receive {:create_worker, ^expected_foreman, worker_id, :materializer}
      assert is_binary(worker_id)

      # Epoch lock on the created worker, at the distributor's epoch.
      assert_receive {:lock, {:stub_worker_ref, node}, 42}
      assert node == node()

      # Unlock with the durable version the worker reported at lock time
      # (zero: a new worker's store is empty, so it replays the shard's full
      # history) and a TSL filtered to the shard's logs.
      zero = Version.zero()
      assert_receive {:unlock, ^stub, ^zero, tsl}
      assert tsl.epoch == 42
      assert tsl.logs == %{"log_a" => [1]}
      assert tsl.services == %{"svc" => %{kind: :log}}

      # TSL delta applied at the director with the current epoch.
      assert_receive {:apply_tsl_delta, %{1 => ^stub}, 42}

      # The parked read drains through the recruited materializer.
      assert {:ok, "red"} = Task.await(task, 5_000)

      # Telemetry: started and succeeded (with duration, tag, node).
      assert_receive {:telemetry, [:bedrock, :distributor, :recruitment, :started], %{},
                      %{cluster: TestCluster, epoch: 42, tag: 1}}

      this_node = node()

      assert_receive {:telemetry, [:bedrock, :distributor, :recruitment, :succeeded], %{duration_us: duration},
                      %{cluster: TestCluster, epoch: 42, tag: 1, node: ^this_node}}

      assert is_integer(duration) and duration >= 0

      # The in-flight marker is cleared.
      assert %State{pending_demands: pending} = :sys.get_state(distributor)
      refute MapSet.member?(pending, 1)
    end

    test "a second demand while recruitment is in flight does not start another" do
      test_pid = self()
      stub = start_stub(%{})

      recruitment = %{
        create_worker_fn: fn _foreman, _worker_id, _kind, _opts ->
          send(test_pid, {:create_worker_called, self()})
          # Hold the recruitment in flight until the test releases it.
          receive do
            :release -> {:ok, :stub_worker_ref}
          end
        end,
        lock_materializer_fn: fn _worker, _epoch -> {:ok, stub, %{}} end,
        unlock_materializer_fn: fn _pid, _durable_version, _tsl -> :ok end
      }

      {distributor, _director} = start_distributor(recruitment: recruitment)

      GenServer.cast(distributor, {:coverage_demand, 1})
      assert_receive {:create_worker_called, task_pid}

      GenServer.cast(distributor, {:coverage_demand, 1})
      refute_receive {:create_worker_called, _}, 100

      send(task_pid, :release)
      assert_receive {:apply_tsl_delta, %{1 => ^stub}, 42}, 1_000
    end
  end

  describe "failed recruitment" do
    test "failure sheds the parked read, emits telemetry, and backs off until expiry" do
      attach_recruitment_telemetry(self())
      test_pid = self()

      recruitment = %{
        create_worker_fn: fn _foreman, _worker_id, _kind, _opts ->
          send(test_pid, :create_worker_called)
          {:error, :no_capacity}
        end
      }

      {distributor, _director} = start_distributor(recruitment: recruitment, backoff_ms: 200)
      placeholder = placeholder_of(distributor)

      task = park_read(placeholder, "apple")
      assert_receive :create_worker_called
      assert {:error, :unavailable} = Task.await(task, 5_000)

      assert_receive {:telemetry, [:bedrock, :distributor, :recruitment, :failed], %{duration_us: _},
                      %{tag: 1, reason: {:worker_creation_failed, :no_capacity, 1, _node}}}

      # Re-demand within the backoff window: no new recruitment attempt,
      # even though the previous one failed.
      GenServer.cast(distributor, {:coverage_demand, 1})
      refute_receive :create_worker_called, 100

      # After the backoff expires, a fresh demand retries recruitment.
      Process.sleep(150)
      GenServer.cast(distributor, {:coverage_demand, 1})
      assert_receive :create_worker_called, 1_000
    end

    test "no materializer-capable node fails recruitment" do
      attach_recruitment_telemetry(self())
      {distributor, _director} = start_distributor(node_capabilities: %{})
      placeholder = placeholder_of(distributor)

      task = park_read(placeholder, "apple")

      assert {:error, :unavailable} = Task.await(task, 5_000)

      assert_receive {:telemetry, [:bedrock, :distributor, :recruitment, :failed], %{duration_us: _},
                      %{tag: 1, reason: :no_materializer_capable_nodes}}

      assert %State{backoff: backoff} = :sys.get_state(distributor)
      assert Map.has_key?(backoff, 1)
    end

    test "a rejected TSL delta (newer epoch exists) stops the distributor" do
      test_pid = self()
      stub = start_stub(%{})

      director =
        start_supervised!(%{
          id: :stale_director,
          start: {CaptureDirector, :start_link, [test_pid, {:error, :newer_epoch_exists}]}
        })

      recruitment = %{
        create_worker_fn: fn _foreman, _worker_id, _kind, _opts -> {:ok, :stub_worker_ref} end,
        lock_materializer_fn: fn _worker, _epoch -> {:ok, stub, %{}} end,
        unlock_materializer_fn: fn _pid, _durable_version, _tsl -> :ok end
      }

      # The shard starts covered so the startup coverage sweep publishes
      # nothing; the rejected delta under test is recruitment's own.
      {distributor, _director} =
        start_distributor(
          recruitment: recruitment,
          director: director,
          transaction_system_layout: %{shard_materializers: %{1 => stub}}
        )

      ref = Process.monitor(distributor)

      GenServer.cast(distributor, {:coverage_demand, 1})

      assert_receive {:DOWN, ^ref, :process, ^distributor, :normal}, 5_000
    end
  end

  describe "epoch change notifications" do
    test "an older-epoch notification is logged as suspicious and ignored" do
      {distributor, _director} = start_distributor([])

      log =
        capture_log(fn ->
          :ok = Distributor.notify_epoch_change(distributor, 41)
          # Synchronize on the mailbox so the cast is processed.
          assert :ok = Distributor.check_epoch(distributor, 42)
        end)

      assert log =~ "suspicious"
      assert log =~ "older epoch 41"
      assert Process.alive?(distributor)
    end
  end
end
