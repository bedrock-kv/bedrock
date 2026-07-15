defmodule Bedrock.ControlPlane.Distributor.ReadoptionTest do
  @moduledoc """
  Unit tests for the Distributor's post-sweep materializer re-adoption:
  after the coverage sweep publishes placeholder coverage, materializers the
  director's services map already names are asked for their shard identity
  and - when they match a swept tag - epoch-locked/unlocked and delivered
  into the slot through the same path recruitment uses, upgrading the
  placeholder to a real pid without creating a fresh worker.

  The identity query runs through the real `Materializer.info` protocol
  against a `StubMaterializer` (or an injected `info_fn`); lock/unlock run
  through the same injectable seams recruitment uses.
  """
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Distributor
  alias Bedrock.ControlPlane.Distributor.State
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.ControlPlane.StubMaterializer

  defmodule TestCluster do
    @moduledoc false
    def otp_name(component), do: :"distributor_readoption_unit_#{component}"
  end

  defmodule CaptureDirector do
    @moduledoc """
    A stand-in director that captures `apply_tsl_delta` calls, forwards them
    to the test process, and replies with a configured result. A list of
    results is consumed one per call, repeating the last.
    """
    use GenServer

    def start_link({test_pid, reply}), do: GenServer.start_link(__MODULE__, {test_pid, reply})
    def start_link(test_pid) when is_pid(test_pid), do: GenServer.start_link(__MODULE__, {test_pid, :ok})

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call({:apply_tsl_delta, delta, epoch}, _from, {test_pid, replies}) do
      send(test_pid, {:apply_tsl_delta, delta, epoch})
      {reply, rest} = pop_reply(replies)
      {:reply, reply, {test_pid, rest}}
    end

    defp pop_reply([reply]), do: {reply, [reply]}
    defp pop_reply([reply | rest]), do: {reply, rest}
    defp pop_reply(reply), do: {reply, reply}
  end

  # A single data shard (tag 1) covering the whole keyspace.
  @shard_layout %{<<0xFF, 0xFF>> => {1, <<>>}}
  @epoch 42
  @version Version.from_integer(1)

  defp unique_otp_name, do: :"distributor_readoption_unit_#{System.unique_integer([:positive])}"

  defp attach_readoption_telemetry(test_pid) do
    handler_id = "readoption-test-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      [
        [:bedrock, :distributor, :readoption, :started],
        [:bedrock, :distributor, :readoption, :succeeded],
        [:bedrock, :distributor, :readoption, :failed]
      ],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  # Starts a registered StubMaterializer that will act as the previous
  # epoch's still-running materializer for the given shard.
  defp start_candidate(opts) do
    name = :"readoption_candidate_#{System.unique_integer([:positive])}"

    pid =
      start_supervised!(%{
        id: {StubMaterializer, name},
        start: {StubMaterializer, :start_link, [Keyword.get(opts, :kvs, %{}), Keyword.put(opts, :name, name)]}
      })

    {name, pid}
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
              epoch: @epoch,
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

  test "sweep publishes the placeholder first, then re-adoption upgrades the slot to the known materializer" do
    attach_readoption_telemetry(self())
    {name, stub} = start_candidate(shard_id: 1, durable_version: Version.from_integer(9), observer: self())

    {distributor, _director} =
      start_distributor(services: %{"m1" => {:materializer, {name, node()}}})

    # Delta ordering: the placeholder covers the slot IMMEDIATELY (the
    # unchanged fast path), the re-adopted pid lands strictly after.
    %State{placeholder: placeholder} = :sys.get_state(distributor)
    assert_receive {:apply_tsl_delta, %{1 => ^placeholder}, @epoch}, 5_000

    # Identity was queried through the real info protocol, then the worker
    # was epoch-locked and unlocked with the durable_version IT reported at
    # lock time (never the cluster-wide recovery version).
    assert_receive {:stub_materializer, {:info, ^stub, fact_names}}, 5_000
    assert :shard_id in fact_names
    assert_receive {:stub_materializer, {:locked_for_recovery, ^stub, @epoch}}, 5_000
    own_durable = Version.from_integer(9)
    assert_receive {:stub_materializer, {:unlocked_after_recovery, ^stub, ^own_durable, unlock_tsl}}, 5_000
    assert unlock_tsl.epoch == @epoch

    assert_receive {:apply_tsl_delta, %{1 => ^stub}, @epoch}, 5_000

    assert_receive {:telemetry, [:bedrock, :distributor, :readoption, :started], %{}, %{tag: 1}}
    assert_receive {:telemetry, [:bedrock, :distributor, :readoption, :succeeded], %{duration_us: _}, %{tag: 1}}

    # No worker creation happened anywhere in this flow, and the slot is now
    # owned by re-adoption: placeholder_tags cleared, materializer monitored.
    wait_until(fn ->
      %State{} = state = :sys.get_state(distributor)

      MapSet.size(state.placeholder_tags) == 0 and
        Enum.any?(state.materializer_monitors, fn {_ref, {tag, pid}} -> tag == 1 and pid == stub end)
    end)

    # Death healing owns the re-adopted materializer: killing it swaps the
    # placeholder back into the slot.
    Process.exit(stub, :kill)
    assert_receive {:apply_tsl_delta, %{1 => ^placeholder}, @epoch}, 5_000
  end

  test "a candidate reporting a different shard_id is left alone and the tag stays on the placeholder" do
    attach_readoption_telemetry(self())
    {name, _stub} = start_candidate(shard_id: 7, observer: self())

    {distributor, _director} =
      start_distributor(services: %{"m1" => {:materializer, {name, node()}}})

    %State{placeholder: placeholder} = :sys.get_state(distributor)
    assert_receive {:apply_tsl_delta, %{1 => ^placeholder}, @epoch}, 5_000
    assert_receive {:stub_materializer, {:info, _pid, _facts}}, 5_000

    # No lock, no second delta, no re-adoption telemetry: the mismatching
    # candidate is not adopted, and demand-driven recruitment remains the
    # only path off the placeholder.
    refute_receive {:stub_materializer, {:locked_for_recovery, _, _}}, 200
    refute_receive {:apply_tsl_delta, _, _}, 100
    refute_receive {:telemetry, [:bedrock, :distributor, :readoption, :started], _, _}, 10

    assert %State{placeholder_tags: tags} = :sys.get_state(distributor)
    assert MapSet.member?(tags, 1)
  end

  test "a dead candidate (info timeout) never delays anything: hard deadline, placeholder stays, sweep unaffected" do
    attach_readoption_telemetry(self())
    test_pid = self()

    # The candidate never answers its identity query; the deadline must cut
    # the identification short instead of hanging re-adoption.
    recruitment = %{
      info_fn: fn _worker, _facts, _opts ->
        send(test_pid, :info_requested)
        Process.sleep(60_000)
      end,
      readoption_deadline_ms: 100
    }

    {distributor, _director} =
      start_distributor(
        services: %{"m1" => {:materializer, {:nonexistent_worker, node()}}},
        recruitment: recruitment
      )

    %State{placeholder: placeholder} = :sys.get_state(distributor)
    assert_receive {:apply_tsl_delta, %{1 => ^placeholder}, @epoch}, 5_000
    assert_receive :info_requested, 5_000

    # Past the deadline: no adoption happened, the distributor is live and
    # still serving its normal duties, and the slot stays on the placeholder.
    refute_receive {:telemetry, [:bedrock, :distributor, :readoption, :started], _, _}, 500
    assert Process.alive?(distributor)
    assert %State{placeholder_tags: tags} = :sys.get_state(distributor)
    assert MapSet.member?(tags, 1)
  end

  test "a tag whose demand-driven recruitment is in flight is skipped (no double coverage)" do
    attach_readoption_telemetry(self())
    test_pid = self()
    {name, _stub} = start_candidate(shard_id: 1, observer: self())

    # Identification blocks until the test releases it, giving a demand-driven
    # recruitment time to get in flight for the same tag.
    recruitment = %{
      info_fn: fn _worker, _facts, _opts ->
        send(test_pid, {:info_requested, self()})

        receive do
          :release -> Materializer.info({name, node()}, [:shard_id, :kind], timeout_in_ms: 1_000)
        end
      end,
      readoption_deadline_ms: 30_000,
      create_worker_fn: fn _foreman, _worker_id, :materializer, _opts ->
        send(test_pid, :create_worker_called)
        Process.sleep(:infinity)
      end
    }

    {distributor, _director} =
      start_distributor(services: %{"m1" => {:materializer, {name, node()}}}, recruitment: recruitment)

    %State{placeholder: placeholder} = :sys.get_state(distributor)
    assert_receive {:apply_tsl_delta, %{1 => ^placeholder}, @epoch}, 5_000
    assert_receive {:info_requested, info_task}, 5_000

    # A real read parks at the placeholder and demands coverage; recruitment
    # for tag 1 is now in flight (its create_worker call never returns).
    _read = Task.async(fn -> Materializer.get(placeholder, "apple", @version, timeout: 5_000) end)
    assert_receive :create_worker_called, 5_000

    wait_until(fn ->
      %State{pending_demands: pending} = :sys.get_state(distributor)
      MapSet.member?(pending, 1)
    end)

    # Now the candidate identifies as tag 1 - but the in-flight recruitment
    # owns the tag, so re-adoption must not lock or double-cover it.
    send(info_task, :release)

    refute_receive {:stub_materializer, {:locked_for_recovery, _, _}}, 500
    refute_receive {:telemetry, [:bedrock, :distributor, :readoption, :started], _, _}, 10
    refute_receive {:apply_tsl_delta, _, _}, 10
  end

  test "an epoch rejection on the re-adoption delta stops the distributor" do
    {name, _stub} = start_candidate(shard_id: 1)

    director =
      start_supervised!(
        {CaptureDirector, {self(), [:ok, {:error, :newer_epoch_exists}]}},
        id: :capture_director
      )

    {distributor, _director} =
      start_distributor(director: director, services: %{"m1" => {:materializer, {name, node()}}})

    ref = Process.monitor(distributor)

    %State{placeholder: placeholder} = :sys.get_state(distributor)
    assert_receive {:apply_tsl_delta, %{1 => ^placeholder}, @epoch}, 5_000
    assert_receive {:apply_tsl_delta, %{1 => _adopted}, @epoch}, 5_000

    # The re-adoption delta was rejected as superseded: this distributor
    # cedes to the newer epoch's distributor.
    assert_receive {:DOWN, ^ref, :process, ^distributor, :normal}, 5_000
  end

  test "re-adoption failure leaves the placeholder covering the tag and demand recruitment as the fallback" do
    attach_readoption_telemetry(self())
    {name, stub} = start_candidate(shard_id: 1, observer: self())

    recruitment = %{
      unlock_materializer_fn: fn _pid, _durable_version, _tsl -> {:error, :pull_wireup_failed} end
    }

    {distributor, _director} =
      start_distributor(services: %{"m1" => {:materializer, {name, node()}}}, recruitment: recruitment)

    %State{placeholder: placeholder} = :sys.get_state(distributor)
    assert_receive {:apply_tsl_delta, %{1 => ^placeholder}, @epoch}, 5_000
    assert_receive {:stub_materializer, {:locked_for_recovery, ^stub, @epoch}}, 5_000

    assert_receive {:telemetry, [:bedrock, :distributor, :readoption, :failed], %{duration_us: _},
                    %{tag: 1, reason: {:unlock_failed, :pull_wireup_failed, _node}}},
                   5_000

    # One shot only: no retry, no backoff, tag remains placeholder-covered
    # and free for demand-driven recruitment.
    wait_until(fn ->
      %State{} = state = :sys.get_state(distributor)

      MapSet.member?(state.placeholder_tags, 1) and
        not MapSet.member?(state.pending_demands, 1) and
        not Map.has_key?(state.backoff, 1)
    end)

    refute_receive {:stub_materializer, {:locked_for_recovery, _, _}}, 200
  end

  test "re-adoption runs once per sweep: a placeholder restart republish does not re-trigger it" do
    test_pid = self()

    recruitment = %{
      info_fn: fn _worker, _facts, _opts ->
        send(test_pid, :info_requested)
        # Mismatch: the tag stays placeholder-covered across the restart.
        {:ok, %{shard_id: 7}}
      end
    }

    {distributor, _director} =
      start_distributor(services: %{"m1" => {:materializer, {:some_worker, node()}}}, recruitment: recruitment)

    %State{placeholder: placeholder} = :sys.get_state(distributor)
    assert_receive {:apply_tsl_delta, %{1 => ^placeholder}, @epoch}, 5_000
    assert_receive :info_requested, 5_000

    # Kill the placeholder: the distributor restarts it and republishes the
    # placeholder-owned slots - which must NOT re-run re-adoption.
    Process.exit(placeholder, :kill)

    assert_receive {:apply_tsl_delta, %{1 => new_placeholder}, @epoch}, 5_000
    assert new_placeholder != placeholder
    refute_receive :info_requested, 500
  end

  defp wait_until(fun, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    fn ->
      if fun.() do
        :ok
      else
        if System.monotonic_time(:millisecond) > deadline do
          flunk("condition not met within #{timeout}ms")
        end

        Process.sleep(10)
        :retry
      end
    end
    |> Stream.repeatedly()
    |> Enum.find(&(&1 == :ok))
  end
end
