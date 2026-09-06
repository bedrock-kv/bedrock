defmodule Bedrock.ControlPlane.Director.DistributorRecruitmentTest do
  @moduledoc """
  The director recruits the per-epoch Distributor singleton after
  recovery accepts commits (FDB: CC recruits DD post-recovery), and
  supervises by monitor: ceded exits are final for the epoch; failures
  retry. The lock — not this supervision — fences a stale instance.
  """
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Config.Parameters
  alias Bedrock.ControlPlane.Director.Recovery
  alias Bedrock.ControlPlane.Director.State

  defp running_state(overrides) do
    struct!(
      %State{
        state: :running,
        epoch: 5,
        cluster: __MODULE__,
        config: %{parameters: Parameters.new([node()])},
        transaction_system_layout: %{
          epoch: 5,
          sequencer: self(),
          proxies: [self()],
          resolvers: [],
          logs: %{}
        },
        distributor_retry_ms: 10
      },
      overrides
    )
  end

  describe "maybe_start_distributor/1" do
    test "recruits once the transaction system is running, and monitors" do
      stub = spawn(fn -> Process.sleep(:infinity) end)
      t = running_state(distributor_start_fn: fn _opts -> {:ok, stub} end)

      t = Recovery.maybe_start_distributor(t)

      assert t.distributor == stub
      assert is_reference(t.distributor_monitor)
    end

    test "hands the distributor the epoch's wiring" do
      test_pid = self()
      stub = spawn(fn -> Process.sleep(:infinity) end)

      t =
        running_state(
          distributor_start_fn: fn opts ->
            send(test_pid, {:started_with, opts})
            {:ok, stub}
          end
        )

      Recovery.maybe_start_distributor(t)

      assert_received {:started_with, opts}
      assert opts[:epoch] == 5
      assert opts[:proxies] == [self()]
      assert opts[:director] == self()
    end

    # bedrock-q67.21.8: idle spin-down is a per-worker policy param, and
    # the distributor's recruits are the only materializers that carry
    # it (Recruitment.worker_params/2 drops it again for tag 0).
    test "hands the distributor the cluster's materializer idle timeout as a worker param" do
      test_pid = self()
      stub = spawn(fn -> Process.sleep(:infinity) end)

      t =
        running_state(
          config: %{parameters: %{materializer_idle_timeout_ms: 60_000}},
          distributor_start_fn: fn opts ->
            send(test_pid, {:started_with, opts})
            {:ok, stub}
          end
        )

      Recovery.maybe_start_distributor(t)

      assert_received {:started_with, opts}
      assert opts[:recruitment_ctx].worker_params == %{"idle_timeout" => 60_000}
    end

    test "leaves an existing distributor alone" do
      existing = spawn(fn -> Process.sleep(:infinity) end)

      t =
        running_state(
          distributor: existing,
          distributor_start_fn: fn _ -> flunk("must not start a second distributor") end
        )

      assert Recovery.maybe_start_distributor(t).distributor == existing
    end

    test "does not recruit before the system is running" do
      t =
        running_state(
          state: :recovering,
          distributor_start_fn: fn _ -> flunk("must not recruit mid-recovery") end
        )

      assert Recovery.maybe_start_distributor(t).distributor == nil
    end

    test "a failed start schedules a retry" do
      t = running_state(distributor_start_fn: fn _ -> {:error, :nope} end)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert Recovery.maybe_start_distributor(t).distributor == nil
          assert_receive {:timeout, :start_distributor}, 200
        end)

      assert log =~ "Distributor start failed"
    end
  end

  describe "handle_distributor_down/2" do
    test "a ceded (:normal) exit is final for the epoch — no re-recruit" do
      t = running_state(distributor: self(), distributor_monitor: make_ref())

      t = Recovery.handle_distributor_down(t, :normal)

      assert t.distributor == nil
      assert t.distributor_monitor == nil
      refute_receive {:timeout, :start_distributor}, 50
    end

    test "a failure schedules a re-recruit — the director itself survives" do
      t = running_state(distributor: self(), distributor_monitor: make_ref())

      t = Recovery.handle_distributor_down(t, {:shutdown, {:lock_take_failed, :whatever}})

      assert t.distributor == nil
      assert_receive {:timeout, :start_distributor}, 200
    end
  end
end
