defmodule Bedrock.ControlPlane.Distributor.RecruitmentIntegrationTest do
  @moduledoc """
  Closes the Phase A loop end-to-end: a client read through the full
  PointReads → StorageRacing → LayoutIndex path against a
  placeholder-covered shard triggers coverage demand, the Distributor's
  recruitment plumbing runs for real (placement, epoch lock/unlock, TSL
  delta to the director), the SAME parked read returns the real value, and
  a subsequent read goes direct once the layout reflects the new
  materializer.

  Only the foreman worker-creation boundary is stubbed: the "created"
  worker is a pre-registered process speaking the real materializer
  lock/unlock and read protocols (a StubMaterializer, and in the final
  test a real olivine materializer on a tmp dir).
  """
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Distributor
  alias Bedrock.ControlPlane.Distributor.State, as: DistributorState
  alias Bedrock.DataPlane.Materializer.Olivine
  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.TransactionBuilder.LayoutIndex
  alias Bedrock.Internal.TransactionBuilder.PointReads
  alias Bedrock.Internal.TransactionBuilder.State
  alias Bedrock.Test.ControlPlane.StubMaterializer

  defmodule TestCluster do
    @moduledoc false
    def otp_name(component), do: :"recruitment_integration_#{component}"
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
  @epoch 42

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
              otp_name: :"recruitment_integration_#{System.unique_integer([:positive])}"
            ],
            opts
          )
        )
      )

    %DistributorState{placeholder: placeholder} = :sys.get_state(pid)
    {pid, placeholder}
  end

  defp client_state(materializer_pid, read_version) do
    layout_index =
      LayoutIndex.build_index(%{
        shard_layout: @shard_layout,
        shard_materializers: %{1 => materializer_pid}
      })

    %State{
      state: :valid,
      layout_index: layout_index,
      read_version: read_version,
      fetch_timeout_in_ms: 5_000
    }
  end

  test "a read against a placeholder-covered shard recruits a materializer and completes" do
    # The "node's worker": pre-registered so the (stubbed) foreman boundary
    # can hand back its ref; it speaks the real lock/unlock protocol.
    worker_name = :"recruited_stub_#{System.unique_integer([:positive])}"

    start_supervised!(%{
      id: :recruited_stub,
      start: {StubMaterializer, :start_link, [%{"apple" => "red"}, [name: worker_name, observer: self()]]}
    })

    # Thin seam at the foreman boundary ONLY: worker creation returns the
    # ref of the worker "started" on the chosen node. Placement, epoch
    # lock/unlock, and the TSL delta all run for real.
    recruitment = %{
      create_worker_fn: fn {_foreman_name, _node}, _worker_id, :materializer, _opts -> {:ok, worker_name} end
    }

    {_distributor, placeholder} = start_distributor(recruitment: recruitment)

    # A read through the full client path against the placeholder-covered shard.
    state = client_state(placeholder, Version.from_integer(1))
    task = Task.async(fn -> PointReads.get_key(state, "apple") end)

    # Recruitment ran the real epoch calls against the worker...
    assert_receive {:stub_materializer, {:locked_for_recovery, worker_pid, @epoch}}, 5_000
    zero = Version.zero()
    assert_receive {:stub_materializer, {:unlocked_after_recovery, ^worker_pid, ^zero, unlock_tsl}}
    assert unlock_tsl.epoch == @epoch

    # ...and applied the TSL delta at the director with the current epoch.
    assert_receive {:apply_tsl_delta, %{1 => ^worker_pid}, @epoch}, 5_000

    # The SAME read (parked at the placeholder) returns the real value.
    assert {%State{}, {:ok, {"apple", "red"}}} = Task.await(task, 5_000)

    # Once the TSL update propagates, a subsequent read goes direct to the
    # recruited materializer - no placeholder, no new demand.
    direct_state = client_state(worker_pid, Version.from_integer(1))
    assert {%State{}, {:ok, {"apple", "red"}}} = PointReads.get_key(direct_state, "apple")
    refute_receive {:stub_materializer, {:locked_for_recovery, _, _}}, 100
  end

  test "recruits a REAL olivine materializer and serves a read through it" do
    tmp_dir = "/tmp/recruitment_olivine_#{System.unique_integer([:positive])}"
    File.mkdir_p!(tmp_dir)
    on_exit(fn -> File.rm_rf(tmp_dir) end)

    worker_id = "olivine_recruit_#{System.unique_integer([:positive])}"
    worker_name = :"recruited_olivine_#{System.unique_integer([:positive])}"

    start_supervised!(
      Olivine.child_spec(
        otp_name: worker_name,
        foreman: self(),
        id: worker_id,
        path: tmp_dir
      )
    )

    # Wait until the worker reports healthy to its (stub) foreman.
    assert_receive {:"$gen_cast", {:worker_health, ^worker_id, {:ok, worker_pid}}}, 5_000

    # Stub ONLY the foreman worker-creation call; the real
    # Materializer.lock_for_recovery/unlock_after_recovery calls run
    # against the live olivine worker.
    recruitment = %{
      create_worker_fn: fn {_foreman_name, _node}, _worker_id, :materializer, _opts -> {:ok, worker_name} end
    }

    {_distributor, placeholder} = start_distributor(recruitment: recruitment)

    # Read at the durable version the recruit is unlocked with (a fresh
    # cluster's version zero) so the materializer can serve immediately.
    state = client_state(placeholder, Version.zero())
    task = Task.async(fn -> PointReads.get_key(state, "apple") end)

    # Recruitment completes: the TSL delta names the live olivine pid.
    assert_receive {:apply_tsl_delta, %{1 => ^worker_pid}, @epoch}, 10_000

    # The parked read is served through the recruited olivine materializer
    # (no value has ever been written, so it resolves to :not_found - a
    # definitive answer from a live materializer, not a failure).
    assert {%State{}, {:error, :not_found}} = Task.await(task, 10_000)

    # A subsequent read goes direct to the olivine worker.
    direct_state = client_state(worker_pid, Version.zero())
    assert {%State{}, {:error, :not_found}} = PointReads.get_key(direct_state, "apple")
  end
end
