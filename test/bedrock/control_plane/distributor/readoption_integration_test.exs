defmodule Bedrock.ControlPlane.Distributor.ReadoptionIntegrationTest do
  @moduledoc """
  Closes the warm-recovery re-adoption loop end-to-end: a distributor comes
  up in a warm-cluster-like state - the data shard's TSL slot is empty but
  the director's services map names a live REAL olivine materializer left
  over from the previous epoch (running on a tmp dir, carrying its shard
  assignment). The sweep publishes placeholder coverage immediately, then
  re-adoption identifies the worker through the real `Materializer.info`
  `:shard_id` fact, re-locks it at the new epoch, and upgrades the slot to
  its pid - and a client read through the full PointReads → StorageRacing →
  LayoutIndex path is served WITHOUT any worker creation.
  """
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Distributor
  alias Bedrock.ControlPlane.Distributor.State, as: DistributorState
  alias Bedrock.DataPlane.Materializer.Olivine
  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.TransactionBuilder.LayoutIndex
  alias Bedrock.Internal.TransactionBuilder.PointReads
  alias Bedrock.Internal.TransactionBuilder.State

  defmodule TestCluster do
    @moduledoc false
    def otp_name(component), do: :"readoption_integration_#{component}"
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

  test "a previous epoch's REAL olivine materializer is re-adopted and serves a read without any worker creation" do
    tmp_dir = "/tmp/readoption_olivine_#{System.unique_integer([:positive])}"
    File.mkdir_p!(tmp_dir)
    on_exit(fn -> File.rm_rf(tmp_dir) end)

    worker_id = "olivine_readopt_#{System.unique_integer([:positive])}"
    worker_name = :"readopted_olivine_#{System.unique_integer([:positive])}"

    # The previous epoch's materializer: a REAL olivine worker whose shard
    # assignment (params, as the foreman's manifest would supply it) makes
    # it identify as the data shard's materializer.
    start_supervised!(
      Olivine.child_spec(
        otp_name: worker_name,
        foreman: self(),
        id: worker_id,
        path: tmp_dir,
        params: %{"shard_id" => 1}
      )
    )

    # Wait until the worker reports healthy to its (stub) foreman.
    assert_receive {:"$gen_cast", {:worker_health, ^worker_id, {:ok, worker_pid}}}, 5_000

    # Worker creation must never happen: re-adoption covers the shard.
    test_pid = self()

    recruitment = %{
      create_worker_fn: fn _foreman, _worker_id, :materializer, _opts ->
        send(test_pid, :create_worker_called)
        {:error, :must_not_create_workers}
      end
    }

    director = start_supervised!({CaptureDirector, self()}, id: :capture_director)

    distributor =
      start_supervised!(
        Distributor.child_spec(
          cluster: TestCluster,
          epoch: @epoch,
          director: director,
          shard_layout: @shard_layout,
          node_capabilities: %{materializer: [node()]},
          durable_version: Version.zero(),
          services: %{worker_id => {:materializer, {worker_name, node()}}},
          recruitment: recruitment,
          otp_name: :"readoption_integration_#{System.unique_integer([:positive])}"
        )
      )

    %DistributorState{placeholder: placeholder} = :sys.get_state(distributor)

    # Placeholder coverage is published IMMEDIATELY (the unchanged fast
    # path), then re-adoption upgrades the slot to the live olivine pid -
    # in that order.
    assert_receive {:apply_tsl_delta, %{1 => ^placeholder}, @epoch}, 5_000
    assert_receive {:apply_tsl_delta, %{1 => ^worker_pid}, @epoch}, 10_000

    # A client read through the full path against the re-adopted layout is
    # served by the re-adopted materializer (no value was ever written, so
    # :not_found is the definitive answer from a live worker).
    direct_state = client_state(worker_pid, Version.zero())
    assert {%State{}, {:error, :not_found}} = PointReads.get_key(direct_state, "apple")

    # And a read that raced the upgrade - parked at the placeholder - is
    # drained to the same materializer rather than triggering recruitment.
    placeholder_state = client_state(placeholder, Version.zero())
    assert {%State{}, {:error, :not_found}} = PointReads.get_key(placeholder_state, "apple")

    # No create_worker call anywhere: the known materializer was taken back
    # into the fold instead of being replaced by a fresh recruit.
    refute_received :create_worker_called
  end
end
