defmodule Bedrock.ControlPlane.Distributor.ServerTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Distributor
  alias Bedrock.ControlPlane.Distributor.State

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
               placeholder: nil
             } = :sys.get_state(pid)

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

  describe "State.check_epoch/2" do
    test "matches, supersedes, and rejects stale callers" do
      state = %State{epoch: 5}

      assert :ok = State.check_epoch(state, 5)
      assert {:error, :epoch_superseded} = State.check_epoch(state, 6)
      assert {:error, :newer_epoch_exists} = State.check_epoch(state, 4)
    end
  end
end
