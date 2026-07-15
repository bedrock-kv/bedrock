defmodule Bedrock.ControlPlane.Distributor.IdleSpindownTest do
  @moduledoc """
  Distributor handling of materializer idle spin-down (bedrock-q67.13):
  a materializer that exits with `{:shutdown, :idle}` gets the placeholder
  swapped into its TSL slot (so coverage never has a hard hole) but is NOT
  eagerly re-recruited - revival is demand-driven, triggered by the next
  read parking at the placeholder. Any other exit reason keeps the eager
  death-healing path.
  """
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Distributor
  alias Bedrock.ControlPlane.Distributor.State
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.ControlPlane.StubMaterializer

  defmodule TestCluster do
    @moduledoc false
    def otp_name(component), do: :"idle_spindown_#{component}"
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

  defp unique_otp_name, do: :"idle_spindown_#{System.unique_integer([:positive])}"

  defp attach_telemetry(test_pid) do
    handler_id = "idle-spindown-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      [
        [:bedrock, :distributor, :materializer_down],
        [:bedrock, :distributor, :idle_spindown],
        [:bedrock, :distributor, :healing, :started]
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

  test "idle exit swaps the placeholder into the slot but does NOT eagerly re-recruit; the next read revives on demand" do
    attach_telemetry(self())
    idle = start_stub(%{"apple" => "red"})
    replacement = start_stub(%{"apple" => "blue"})

    {distributor, _director} = start_distributor(recruitment: queued_recruitment(self(), [idle, replacement]))

    recruit_initial(distributor, idle)
    assert_receive :create_worker_called
    placeholder = placeholder_of(distributor)

    # The materializer spins itself down after read inactivity.
    GenServer.stop(idle, {:shutdown, :idle})

    assert_receive {:telemetry, [:bedrock, :distributor, :materializer_down], %{},
                    %{cluster: TestCluster, epoch: @epoch, tag: 1, reason: {:shutdown, :idle}}}

    assert_receive {:telemetry, [:bedrock, :distributor, :idle_spindown], %{}, %{cluster: TestCluster, tag: 1}}

    # The placeholder is swapped back into the slot...
    assert_receive {:apply_tsl_delta, %{1 => ^placeholder}, @epoch}, 5_000

    # ...but this is not healing, and no eager re-recruitment happens.
    refute_receive {:telemetry, [:bedrock, :distributor, :healing, :started], %{}, %{cluster: TestCluster, tag: 1}},
                   100

    refute_receive :create_worker_called, 200

    # The monitor entry is gone and the tag is placeholder-covered.
    %State{materializer_monitors: monitors, placeholder_tags: tags, healing: healing} = :sys.get_state(distributor)
    assert monitors == %{}
    assert MapSet.member?(tags, 1)
    assert MapSet.size(healing) == 0

    # The next read parks at the placeholder, demands coverage, and revives
    # the shard through the normal recruitment flow.
    task = park_read(placeholder, "apple")
    assert_receive :create_worker_called, 5_000
    assert_receive {:apply_tsl_delta, %{1 => ^replacement}, @epoch}, 5_000
    assert {:ok, "blue"} = Task.await(task, 5_000)

    # The revived materializer is monitored again.
    wait_until(fn ->
      %State{materializer_monitors: monitors} = :sys.get_state(distributor)
      Map.values(monitors) == [{1, replacement}]
    end)
  end

  test "non-idle exits keep the eager healing path" do
    attach_telemetry(self())
    dying = start_stub(%{"apple" => "red"})
    replacement = start_stub(%{"apple" => "blue"})

    {distributor, _director} = start_distributor(recruitment: queued_recruitment(self(), [dying, replacement]))

    recruit_initial(distributor, dying)
    assert_receive :create_worker_called
    placeholder = placeholder_of(distributor)

    Process.exit(dying, :kill)

    assert_receive {:telemetry, [:bedrock, :distributor, :healing, :started], %{}, %{cluster: TestCluster, tag: 1}}
    assert_receive {:apply_tsl_delta, %{1 => ^placeholder}, @epoch}, 5_000

    # Eager re-recruitment fires without any read demand.
    assert_receive :create_worker_called, 5_000
    assert_receive {:apply_tsl_delta, %{1 => ^replacement}, @epoch}, 5_000
  end

  test "a stale idle-swap completion for a tag already re-covered does not recruit" do
    stub = start_stub(%{"apple" => "red"})
    {distributor, _director} = start_distributor(recruitment: queued_recruitment(self(), [stub]))

    recruit_initial(distributor, stub)
    assert_receive :create_worker_called

    GenServer.cast(distributor, {:idle_swap_complete, 1, :ok})

    refute_receive :create_worker_called, 100
    assert Process.alive?(distributor)
  end
end
