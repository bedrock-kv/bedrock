defmodule Bedrock.ControlPlane.Distributor.DeathHealingIntegrationTest do
  @moduledoc """
  Closes the death-healing loop (bedrock-q67.7) end-to-end through the
  full client read path (PointReads → StorageRacing → LayoutIndex): a read
  is served by a recruited materializer, the materializer is killed, the
  next read through the placeholder PARKS (it is not stale-forwarded to
  the corpse), healing re-recruits a replacement, and the SAME parked read
  is served again.

  Only the foreman worker-creation boundary is stubbed: the "created"
  workers are pre-registered StubMaterializer processes speaking the real
  materializer lock/unlock and read protocols.
  """
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Distributor
  alias Bedrock.ControlPlane.Distributor.State, as: DistributorState
  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.TransactionBuilder.LayoutIndex
  alias Bedrock.Internal.TransactionBuilder.PointReads
  alias Bedrock.Internal.TransactionBuilder.State
  alias Bedrock.Test.ControlPlane.StubMaterializer

  defmodule TestCluster do
    @moduledoc false
    def otp_name(component), do: :"death_healing_integration_#{component}"
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

  defp client_state(materializer_pid) do
    layout_index =
      LayoutIndex.build_index(%{
        shard_layout: @shard_layout,
        shard_materializers: %{1 => materializer_pid}
      })

    %State{
      state: :valid,
      layout_index: layout_index,
      read_version: Version.from_integer(1),
      fetch_timeout_in_ms: 5_000
    }
  end

  defp start_worker(kvs) do
    name = :"healed_stub_#{System.unique_integer([:positive])}"

    pid =
      start_supervised!(%{
        id: name,
        start: {StubMaterializer, :start_link, [kvs, [name: name]]},
        restart: :temporary
      })

    {name, pid}
  end

  test "read served, materializer killed, next read parks, healing restores service" do
    {first_name, first_pid} = start_worker(%{"apple" => "red"})
    {second_name, second_pid} = start_worker(%{"apple" => "red"})

    # Stub ONLY the foreman worker-creation boundary; hand out the two
    # pre-registered workers in order across recruitment attempts.
    {:ok, queue} = Agent.start_link(fn -> [first_name, second_name] end)

    recruitment = %{
      create_worker_fn: fn {_foreman_name, _node}, _worker_id, :materializer, _opts ->
        {:ok, Agent.get_and_update(queue, fn [next | rest] -> {next, rest} end)}
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
          otp_name: :"death_healing_integration_#{System.unique_integer([:positive])}",
          recruitment: recruitment
        )
      )

    %DistributorState{placeholder: placeholder} = :sys.get_state(distributor)

    # The startup coverage sweep publishes the placeholder into the (still
    # uncovered) shard slot; consume its delta so the assertions below
    # observe the death-healing swap and not this stale message.
    assert_receive {:apply_tsl_delta, %{1 => ^placeholder}, @epoch}, 5_000

    # 1. A read through the full client path recruits the first worker and
    #    is served by it.
    state = client_state(placeholder)
    task = Task.async(fn -> PointReads.get_key(state, "apple") end)
    assert_receive {:apply_tsl_delta, %{1 => ^first_pid}, @epoch}, 5_000
    assert {%State{}, {:ok, {"apple", "red"}}} = Task.await(task, 5_000)

    # 2. Kill the materializer: the distributor swaps the placeholder into
    #    the slot and begins healing.
    Process.exit(first_pid, :kill)
    assert_receive {:apply_tsl_delta, %{1 => ^placeholder}, @epoch}, 5_000

    # 3. The next read (stale layout still pointing at the placeholder)
    #    PARKS - a stale-forward to the corpse could never produce a value -
    #    and completes once healing recruits the replacement.
    healed_task = Task.async(fn -> PointReads.get_key(state, "apple") end)
    assert_receive {:apply_tsl_delta, %{1 => ^second_pid}, @epoch}, 5_000
    assert {%State{}, {:ok, {"apple", "red"}}} = Task.await(healed_task, 5_000)

    # 4. Once the layout update propagates, reads go direct to the healed
    #    materializer.
    direct_state = client_state(second_pid)
    assert {%State{}, {:ok, {"apple", "red"}}} = PointReads.get_key(direct_state, "apple")
  end
end
