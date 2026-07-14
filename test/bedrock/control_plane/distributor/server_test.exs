defmodule Bedrock.ControlPlane.Distributor.ServerTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Distributor
  alias Bedrock.ControlPlane.Distributor.State
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.Test.ControlPlane.StubMaterializer

  defmodule TestCluster do
    @moduledoc false
    def otp_name(component), do: :"distributor_server_test_#{component}"
  end

  defp unique_otp_name, do: :"distributor_test_#{System.unique_integer([:positive])}"

  defp attach_telemetry(test_pid) do
    handler_id = "distributor-test-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      [
        [:bedrock, :distributor, :started],
        [:bedrock, :distributor, :stopped]
      ],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp start_director_stub do
    spawn(fn ->
      receive do
        :stop -> :ok
      end
    end)
  end

  defp start_distributor(opts \\ []) do
    director = Keyword.get_lazy(opts, :director, &start_director_stub/0)

    pid =
      start_supervised!(
        Distributor.child_spec(
          cluster: TestCluster,
          epoch: Keyword.get(opts, :epoch, 42),
          director: director,
          shard_layout: Keyword.get(opts, :shard_layout, %{}),
          otp_name: unique_otp_name()
        )
      )

    {pid, director}
  end

  describe "lifecycle" do
    test "starts with the expected state and emits the started telemetry event" do
      attach_telemetry(self())
      shard_layout = %{<<0xFF>> => {0, <<>>}}

      {pid, director} = start_distributor(epoch: 42, shard_layout: shard_layout)

      assert %State{
               cluster: TestCluster,
               epoch: 42,
               director: ^director,
               shard_layout: ^shard_layout,
               materializer_monitors: %{},
               placeholder: placeholder
             } = :sys.get_state(pid)

      assert is_pid(placeholder) and Process.alive?(placeholder)

      assert_receive {:telemetry, [:bedrock, :distributor, :started], %{},
                      %{cluster: TestCluster, epoch: 42, director: ^director}}
    end

    test "stops with :normal and emits the stopped telemetry event when the director exits" do
      attach_telemetry(self())
      {pid, director} = start_distributor(epoch: 7)
      ref = Process.monitor(pid)

      send(director, :stop)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

      assert_receive {:telemetry, [:bedrock, :distributor, :stopped], %{},
                      %{cluster: TestCluster, epoch: 7, reason: :normal}}
    end
  end

  describe "epoch guard" do
    test "check_epoch with the distributor's own epoch returns :ok" do
      {pid, _director} = start_distributor(epoch: 42)

      assert :ok = Distributor.check_epoch(pid, 42)
      assert Process.alive?(pid)
    end

    test "check_epoch from a stale caller returns an error and leaves the distributor running" do
      {pid, _director} = start_distributor(epoch: 42)

      assert {:error, :newer_epoch_exists} = Distributor.check_epoch(pid, 41)
      assert Process.alive?(pid)
    end

    test "check_epoch with a newer epoch stops the distributor with :normal" do
      attach_telemetry(self())
      {pid, _director} = start_distributor(epoch: 42)
      ref = Process.monitor(pid)

      assert {:error, :epoch_superseded} = Distributor.check_epoch(pid, 43)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
      assert_receive {:telemetry, [:bedrock, :distributor, :stopped], %{}, %{epoch: 42, reason: :normal}}
    end

    test "notify_epoch_change with a newer epoch stops the distributor with :normal" do
      attach_telemetry(self())
      {pid, _director} = start_distributor(epoch: 42)
      ref = Process.monitor(pid)

      :ok = Distributor.notify_epoch_change(pid, 43)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
      assert_receive {:telemetry, [:bedrock, :distributor, :stopped], %{}, %{epoch: 42, reason: :normal}}
    end

    test "notify_epoch_change with the same or an older epoch is ignored" do
      {pid, _director} = start_distributor(epoch: 42)

      :ok = Distributor.notify_epoch_change(pid, 42)
      :ok = Distributor.notify_epoch_change(pid, 41)

      # Synchronize on the mailbox to be sure both casts were processed.
      assert :ok = Distributor.check_epoch(pid, 42)
      assert Process.alive?(pid)
    end
  end

  describe "placeholder supervision" do
    test "the placeholder dies with the distributor" do
      {pid, director} = start_distributor()
      %State{placeholder: placeholder} = :sys.get_state(pid)
      ref = Process.monitor(placeholder)

      send(director, :stop)

      assert_receive {:DOWN, ^ref, :process, ^placeholder, :shutdown}
    end

    test "the placeholder is restarted when it dies" do
      {pid, _director} = start_distributor()
      %State{placeholder: placeholder} = :sys.get_state(pid)
      ref = Process.monitor(placeholder)

      Process.exit(placeholder, :kill)
      assert_receive {:DOWN, ^ref, :process, ^placeholder, :killed}

      # Synchronize on the distributor's mailbox, then check the new pid.
      assert :ok = Distributor.check_epoch(pid, 42)
      assert %State{placeholder: new_placeholder} = :sys.get_state(pid)
      assert is_pid(new_placeholder)
      assert new_placeholder != placeholder
      assert Process.alive?(new_placeholder)
    end
  end

  describe "coverage demand and delivery" do
    defp attach_demand_telemetry(test_pid) do
      handler_id = "distributor-demand-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:bedrock, :distributor, :coverage_demand],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)
    end

    test "coverage_demand is remembered and emits telemetry" do
      attach_demand_telemetry(self())
      {pid, _director} = start_distributor()

      GenServer.cast(pid, {:coverage_demand, 7})

      assert_receive {:telemetry, [:bedrock, :distributor, :coverage_demand], %{}, %{cluster: TestCluster, tag: 7}}

      assert %State{pending_demands: pending} = :sys.get_state(pid)
      assert MapSet.member?(pending, 7)
    end

    test "deliver_coverage relays to the placeholder and clears the pending demand" do
      attach_demand_telemetry(self())
      shard_layout = %{<<0xFF, 0xFF>> => {1, <<>>}}
      {pid, _director} = start_distributor(shard_layout: shard_layout)
      %State{placeholder: placeholder} = :sys.get_state(pid)

      stub =
        start_supervised!(%{
          id: {StubMaterializer, System.unique_integer([:positive])},
          start: {StubMaterializer, :start_link, [%{"apple" => "red"}]}
        })

      # Park a read against the placeholder, then deliver coverage through
      # the distributor's internal seam and observe the drain.
      version = Bedrock.DataPlane.Version.from_integer(1)

      task =
        Task.async(fn ->
          Materializer.get(placeholder, "apple", version, timeout: 1_000)
        end)

      # Wait for the placeholder's demand to be processed by the distributor
      # before delivering coverage, so the pending set is stable.
      assert_receive {:telemetry, [:bedrock, :distributor, :coverage_demand], %{}, %{tag: 1}}

      :ok = Distributor.deliver_coverage(pid, 1, stub)

      assert {:ok, "red"} = Task.await(task, 1_000)

      assert %State{pending_demands: pending} = :sys.get_state(pid)
      refute MapSet.member?(pending, 1)
    end

    test "fail_coverage relays the failure to the placeholder and clears the pending demand" do
      attach_demand_telemetry(self())
      shard_layout = %{<<0xFF, 0xFF>> => {1, <<>>}}
      {pid, _director} = start_distributor(shard_layout: shard_layout)
      %State{placeholder: placeholder} = :sys.get_state(pid)

      version = Bedrock.DataPlane.Version.from_integer(1)

      task =
        Task.async(fn ->
          Materializer.get(placeholder, "apple", version, timeout: 1_000)
        end)

      assert_receive {:telemetry, [:bedrock, :distributor, :coverage_demand], %{}, %{tag: 1}}

      :ok = Distributor.fail_coverage(pid, 1, :no_capacity)

      assert {:error, :unavailable} = Task.await(task, 1_000)

      assert %State{pending_demands: pending} = :sys.get_state(pid)
      refute MapSet.member?(pending, 1)
    end
  end

  describe "State.check_epoch/2" do
    test "matches, supersedes, and rejects stale callers" do
      state = %State{epoch: 5}

      assert :ok = State.check_epoch(state, 5)
      assert {:error, :epoch_superseded} = State.check_epoch(state, 6)
      assert {:error, :newer_epoch_exists} = State.check_epoch(state, 4)
    end
  end
end
